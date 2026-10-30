# bazaar-contrib-claude-code

Claude Code Harness provider for [Bazaar Compute Node](https://github.com/yuchanns/bazaar-compute-node).

## Setup

Install and configure the Claude Code CLI, then install BCN and this provider:

```sh
uv tool install bazaar-compute-node
bcn contrib add bazaar-contrib-claude-code --enable
```

Add an agent using an installed Channel provider:

```sh
bcn agent add --name Editor --channel CHANNEL_KIND --harness claudecode
bcn run
```

## Configuration

Optional fields in the agent's `claudecode` Harness configuration:

- `model`: Claude Code model; defaults to the Claude Code configuration.
- `effort`: Reasoning effort; defaults to the Claude Code configuration.
- `api_key`: API key injected as `ANTHROPIC_API_KEY`, overriding the inherited value.
- `bedrock_api_key`: Amazon Bedrock API key injected as `AWS_BEARER_TOKEN_BEDROCK`
  together with `CLAUDE_CODE_USE_BEDROCK=1`, overriding the inherited values.
- `bedrock_region`: AWS region injected as `AWS_REGION`; without it Claude Code reads
  `AWS_REGION`, `AWS_DEFAULT_REGION` or the AWS profile, then defaults to `us-east-1`.
