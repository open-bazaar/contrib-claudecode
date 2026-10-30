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
