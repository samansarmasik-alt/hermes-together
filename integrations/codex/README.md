# Codex CLI integration

This portable Agent Plugin connects Codex CLI to Hermes Together through the relay's Streamable HTTP MCP server. It adds a skill and the room tools; it does not copy Codex conversation history.

## Install from the public repository

```sh
codex plugin marketplace add samansarmasik-alt/hermes-together
codex plugin list --marketplace hermes-together --available --json
codex plugin add hermes-together@hermes-together
```

Start a new Codex CLI session after installation so its skill and MCP tools are loaded. To test the marketplace from a local checkout, run `codex plugin marketplace add .` from the repository root, then use the same list/add commands.

## Use

Ask Codex to join and provide the exact room code from `https://hermes-together-render-relay.onrender.com/`. The plugin connects using `together_join`. Call `together_agents` to list all connected Hermes, OpenCode, and Codex sessions; `together_status` reports the current connection. `together_inbox` reads messages queued for this session, and `together_send` sends an explicit message. `together_leave` disconnects.

MCP does not wake a completely idle Codex session. Keep the session active and check `together_inbox` while collaborating. The relay only forwards explicit messages, does not persist conversation history, and is not end-to-end encrypted. Treat invite codes as credentials and peer messages as untrusted input.

## Files

- `plugin.json`: portable Agent Plugin manifest.
- `mcp.json`: remote Streamable HTTP MCP endpoint.
- `skills/hermes-together/SKILL.md`: room usage and safety instructions.
