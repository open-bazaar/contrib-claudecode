// Warm one real, model-visible prefix before forwarding the acceptance traffic.
import http from 'node:http';
import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { Transform } from 'node:stream';

const upstream = new URL(process.argv[2]);
const evidence = '/evidence/preheat.jsonl';
const agent = new http.Agent({ keepAlive: true });
let warmup;

function record(value) {
  appendFileSync(evidence, `${JSON.stringify({ at: new Date().toISOString(), ...value })}\n`, { mode: 0o600 });
}

const sha256 = body => createHash('sha256').update(body).digest('hex');

function post(path, body, connection = { hostname: upstream.hostname, port: upstream.port }) {
  const bytes = Buffer.from(JSON.stringify(body));
  return new Promise((resolve, reject) => {
    const request = http.request({
      ...connection, path, method: 'POST', agent,
      headers: { 'content-type': 'application/json', 'content-length': bytes.length },
    }, response => {
      const chunks = [];
      response.on('data', chunk => chunks.push(chunk));
      response.on('error', reject);
      response.on('end', () => {
        try {
          if (response.statusCode !== 200) throw new Error(`${path} returned ${response.statusCode}`);
          resolve(JSON.parse(Buffer.concat(chunks)));
        } catch (error) {
          reject(error);
        }
      });
    });
    request.on('error', reject);
    request.end(bytes);
  });
}

async function tokens(body) {
  const template = await post('/apply-template', body);
  const result = await post('/tokenize', {
    content: template.prompt, add_special: false, parse_special: true,
  });
  return result.tokens;
}

async function preheat(body, requestId) {
  const user = body.messages.findIndex(message => message.role === 'user');
  if (user < 0) throw new Error('The first acceptance request has no user message');
  // The common prefix ends before any case-specific user content. Template and
  // tokenizer are supplied by the exact running model; no prompt is rewritten.
  const emptyUser = {
    ...body,
    messages: [
      ...body.messages.slice(0, user),
      { ...body.messages[user], content: '' },
    ],
  };
  const [actual, boundary] = await Promise.all([tokens(body), tokens(emptyUser)]);
  let length = 0;
  while (length < actual.length && length < boundary.length && actual[length] === boundary[length]) length++;
  if (!length) throw new Error('No model-token prefix is available for preheat');
  const prefix = actual.slice(0, length);
  const prefixHash = sha256(JSON.stringify(prefix));
  const identity = JSON.parse(readFileSync('/evidence/cache-identity.json', 'utf8'));
  const key = sha256(JSON.stringify({ identity, prefix_sha256: prefixHash }));
  const started = performance.now();
  const found = await post('/restore', { key }, { socketPath: '/evidence/cache.sock' });
  if (found.restored) {
    const restored = await post('/slots/0?action=restore', { filename: 'prefix.bin' });
    const cache = {
      key, created: false, prefix_tokens: length, prefix_sha256: prefixHash,
      elapsed_seconds: (performance.now() - started) / 1000, ...restored,
    };
    record({ kind: 'cache.restored', ...cache });
    writeFileSync('/evidence/cache.json', JSON.stringify(cache), { mode: 0o600 });
    return;
  }
  record({
    kind: 'warmup.start', request_id: requestId, prefix_tokens: length,
    first_request_tokens: actual.length, prefix_sha256: prefixHash,
    tool_count: body.tools?.length ?? 0, n_predict: 1,
  });
  const warming = performance.now();
  // This one generated token is discarded. It cannot invoke a native tool.
  const result = await post('/completion', {
    prompt: prefix, n_predict: 1, stream: false, cache_prompt: true,
  });
  record({
    kind: 'warmup.complete', request_id: requestId, prefix_tokens: length,
    elapsed_seconds: (performance.now() - warming) / 1000,
    timings: result.timings, tokens_cached: result.tokens_cached,
    tokens_evaluated: result.tokens_evaluated, tokens_predicted: result.tokens_predicted,
    discarded_output_characters: result.content?.length ?? 0,
  });
  const saved = await post('/slots/0?action=save', { filename: 'prefix.bin' });
  const cache = {
    key, created: true, prefix_tokens: length, prefix_sha256: prefixHash, ...saved,
  };
  await post('/save', { key }, { socketPath: '/evidence/cache.sock' });
  writeFileSync('/evidence/cache.json', JSON.stringify(cache), { mode: 0o600 });
  record({ kind: 'cache.saved', ...cache });
}

function forward(request, response, bytes, entry) {
  const started = performance.now();
  let pending = '';
  const usage = new Transform({
    transform(chunk, encoding, callback) {
      pending += chunk.toString('utf8');
      let end;
      while ((end = pending.indexOf('\n')) >= 0) {
        const line = pending.slice(0, end).trim();
        pending = pending.slice(end + 1);
        if (line.startsWith('data: ') && line !== 'data: [DONE]') {
          try {
            const value = JSON.parse(line.slice(6));
            if (value.usage) entry.usage = value.usage;
          } catch { /* Forward all original bytes even for non-JSON SSE data. */ }
        }
      }
      callback(null, chunk);
    },
  });
  const call = http.request({
    hostname: upstream.hostname, port: upstream.port, path: request.url,
    method: request.method, agent, headers: { ...request.headers, host: upstream.host },
  }, result => {
    entry.status = result.statusCode;
    entry.headers_elapsed_seconds = (performance.now() - started) / 1000;
    response.writeHead(result.statusCode, result.headers);
    result.on('aborted', () => response.destroy());
    result.on('error', error => response.destroy(error));
    usage.on('error', error => response.destroy(error));
    result.pipe(usage).pipe(response);
  });
  response.on('close', () => {
    entry.elapsed_seconds = (performance.now() - started) / 1000;
    entry.total_elapsed_seconds = entry.elapsed_seconds + (entry.preheat_wait_seconds ?? 0);
    entry.complete = response.writableFinished;
    record({ kind: 'request.complete', ...entry });
    if (!response.writableFinished) call.destroy();
  });
  call.on('error', error => {
    entry.error = error.message;
    if (!response.destroyed) {
      if (!response.headersSent) response.writeHead(502);
      response.end();
    }
  });
  call.end(bytes);
}

const server = http.createServer(async (request, response) => {
  if (request.url === '/__preheat_health') {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ ready: true }));
    return;
  }
  const chunks = [];
  try {
    for await (const chunk of request) chunks.push(chunk);
    const bytes = Buffer.concat(chunks);
    const entry = {
      request_id: randomUUID(), method: request.method, path: request.url,
      request_bytes: bytes.length, request_sha256: sha256(bytes),
    };
    if (request.method === 'POST' && request.url === '/v1/chat/completions') {
      const body = JSON.parse(bytes);
      entry.tool_count = body.tools?.length ?? 0;
      record({ kind: 'request.received', ...entry });
      const waiting = performance.now();
      warmup ??= preheat(body, entry.request_id);
      await warmup;
      entry.preheat_wait_seconds = (performance.now() - waiting) / 1000;
    }
    if (!response.destroyed) forward(request, response, bytes, entry);
  } catch (error) {
    record({ kind: 'error', error: error.message });
    if (!response.destroyed) {
      response.writeHead(502);
      response.end('Acceptance preheat failed; inspect preheat.jsonl');
    }
  }
});

server.listen(8080, '0.0.0.0', () => record({ kind: 'ready', upstream: upstream.origin }));
process.on('SIGTERM', () => {
  server.close();
  server.closeAllConnections();
  agent.destroy();
});
