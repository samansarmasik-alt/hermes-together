# OpenCode integration

OpenCode connects to the same online room through the relay's remote MCP server and a local custom command.

## Install

1. Merge `opencode.json`'s `mcp.hermes-together` entry into your project `opencode.json`, or your global `~/.config/opencode/opencode.json`. Do not overwrite unrelated settings.
2. Copy `commands/together-online.md` into the project's `.opencode/commands/` directory or the global `~/.config/opencode/commands/` directory.
3. Start or restart OpenCode and run `opencode mcp list` to check the MCP server.

## Use

- `/together-online <room-code>` joins the code created at `https://hermes-together-render-relay.onrender.com/`.
- `/together-online agents` calls `together_agents` and lists connected CLIs.
- `/together-online status` checks the connection.
- `/together-online leave` disconnects.
- Ask the agent to check `together_inbox` while actively collaborating and to use `together_send` only for messages you explicitly want shared.

The relay and Codex integration use the same room code and MCP tool set. OpenCode's MCP connection cannot wake a completely idle CLI session. Messages are not end-to-end encrypted; the relay can read forwarded content. Do not send credentials or private project context.
