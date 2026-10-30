import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { restoreCache, saveCache } from '@actions/cache';

const output = path.resolve('.artifacts/preheat');
const directory = path.resolve('.artifacts/prefix-cache');
await mkdir(output, { recursive: true });
await mkdir(directory, { recursive: true });

// The native request supplies its actual prefix key over the existing evidence mount.
// Actions runtime credentials stay in this host action, outside the model proxy.
const server = http.createServer(async (request, response) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const { key } = JSON.parse(Buffer.concat(chunks));
  const identity = `llama-prefix-${process.env.RUNNER_OS}-${key}`;
  if (request.url === '/restore') {
    const restored = await restoreCache([directory], identity);
    response.end(JSON.stringify({ restored: restored === identity }));
  } else {
    const cacheId = await saveCache([directory], identity);
    response.end(JSON.stringify({ cache_id: cacheId }));
  }
});
server.listen(path.join(output, 'cache.sock'));
await once(server, 'listening');

try {
  const child = spawn('uv', [
    'run', '--no-sync', 'python', '-u', 'tests/gateway_acceptance.py',
    '--output', '.artifacts/preheat', '--cache-dir', '.artifacts/prefix-cache', '--',
    'python', '-u', 'tests/gateway_resources.py', '.artifacts/gateway-resources.jsonl',
    'python', '-u', '-m', 'pytest', '-vv', '-s', '-m', 'e2e',
    `tests/test_application.py::test_installed_application[${process.env.INPUT_SCENARIO}]`,
    '--model', process.env.INPUT_MODEL, '--basetemp', '.artifacts/e2e',
  ], { stdio: 'inherit' });
  const [code] = await once(child, 'exit');
  process.exitCode = code ?? 1;
} finally {
  await new Promise(resolve => server.close(resolve));
}
