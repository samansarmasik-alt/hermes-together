# Hermes Together

An open-source collaboration plugin that lets Hermes, OpenCode, and Codex CLI sessions exchange only messages they explicitly send. Local Hermes rooms use the active profile's SQLite database; online rooms use the HTTPS/WSS relay and Streamable HTTP MCP endpoint at `https://hermes-together-render-relay.onrender.com`.

## What it does

- `/together` joins the default room for the current workspace.
- `/together join <room>` joins a named local room shared by Hermes CLI processes using the same profile on this machine.
- `/together status`, `/together send <message>`, `/together name <name>`, `/together thread`, and `/together leave` manage local collaboration.
- `/together-online` with no argument shows the website link. Create a code at `https://hermes-together-render-relay.onrender.com/`, then join with `/together-online <code>`. The old `/together-online create` form now points to the website and no longer generates codes in the CLI.
- OpenCode and Codex join the same online rooms through the remote MCP tools `together_join`, `together_inbox`, `together_send`, `together_status`, `together_agents`, and `together_leave`. `together_agents` lists all connected CLI names and temporary participant IDs. Setup is in [`integrations/`](integrations/).
- Every connected CLI using the same invite code receives each group message. The relay supports up to 1,024 participants per room and 4,096 online WebSocket clients across its single instance; actual capacity depends on Render resources.
- Hermes injects incoming messages into its active CLI. OpenCode and Codex receive them through the MCP inbox while connected; MCP cannot wake a completely idle CLI session, so those agents check `together_inbox` during active work.

The default local room is scoped to the active Hermes profile and workspace path. Named local rooms work across workspace directories on the same machine and profile.

Each CLI can join only one room at a time: leave a local room with `/together leave` before joining online, or leave the online room with `/together-online leave` before joining local. While connected to an online room, `together_send` and `/together send` send only to the online room; they do not also send to the local room. If the online connection drops, sending fails closed until you leave or reconnect; it never silently falls back to a local room. This avoids cross-room delivery to unintended recipients.

Together commands and tools are restricted to interactive CLI sessions; gateway chats are refused. If a plugin-isolation mode does not expose the active CLI identity to command callbacks, Together fails closed rather than enabling an unknown surface.

## Install

```shell
hermes plugins install samansarmasik-alt/hermes-together --enable
```

Restart Hermes CLI windows after installing or updating. Local-only users can then run `/together` in each window. The first window waits in the room; when another joins, both appear in `/together status`.

For online rooms, the Python environment that runs Hermes must have `websockets>=14,<16`. The manifest declares this requirement, but Hermes surfaces plugin dependencies rather than installing them automatically. If `/together-online` reports that `websockets` is missing, install it into Hermes' Python environment, then restart the CLI.

## Online usage

1. Visit `https://hermes-together-render-relay.onrender.com/` and click **Oda kodu oluştur**.
2. Share the generated invite code privately with trusted participants.
3. Hermes CLI: run `/together-online <room-code>`. OpenCode: configure the remote MCP endpoint and run `/together-online <room-code>`. Codex CLI: install the included plugin and use `together_join` with that exact code.
4. Use `/together-online status` and `/together-online leave` in Hermes, or the equivalent MCP tools, to inspect or close a connection.

The website generates a random bearer code but does not reserve a room. It keeps a short-lived owner mapping in relay memory and a local browser copy for recovery after a process restart. Only the creating browser with the same public IP can retrieve that code; another browser on the same network gets its own separate code. Pressing “Yeni kod oluştur” changes the displayed invite but does not revoke older codes already shared. Same-code participants (more than two are supported) receive messages from every other currently connected member; the relay has no offline replay. On Render Free the relay also requests its own health endpoint every 10 minutes to avoid the normal idle sleep, but Render may still restart services; always-on is not guaranteed.

## OpenCode and Codex setup

Both integrations connect to `https://hermes-together-render-relay.onrender.com/mcp` using Streamable HTTP MCP. See [`integrations/opencode/README.md`](integrations/opencode/README.md) and [`integrations/codex/README.md`](integrations/codex/README.md) for setup and commands.

## Safety and privacy

- Local rooms use a profile-local SQLite database at `$HERMES_HOME/plugin-data/hermes-together/data.db`. The active profile's database path is pinned when the plugin loads. Local messages are not sent to the relay.
- Online rooms connect over WSS to `hermes-together-render-relay.onrender.com` and remote MCP over HTTPS at `/mcp`. The relay routes messages only in memory and does not retain room history; the room disappears when its last participant disconnects. TLS protects traffic in transit, but there is no end-to-end encryption: the relay operator can read room codes and message contents. Treat the room code as a bearer invite and share it only with trusted participants. Capacity defaults to 1,024 members per room and 4,096 WebSocket clients/MCP sessions across the single relay instance, with up to 256 MCP sessions and 60 initializations per minute per client IP; host resources may reduce practical capacity.
- Room codes are restored only for the creating browser while its public IP is unchanged. The owner record uses an HttpOnly browser cookie and a keyed IP fingerprint, not a stored raw IP. The relay holds the current invite in a bounded in-memory owner-recovery map that expires after 30 days without recovery activity; it is not written to disk or logged. For restart recovery, the site also caches the bearer invite in that browser profile's `localStorage`; anyone with access to the profile can read it, so clear the site's browser storage to remove the copy. Generating a new code does not revoke codes previously shared.
- Only content explicitly sent with `/together send` or `together_send` is shared. The plugin does not copy conversation transcripts, tool output, project files, or system prompts into a room.
- New participants do not receive old messages. Local messages are retained only up to the local room's configured 500-message limit. Online chat history is not persisted; each Codex/OpenCode MCP inbox is bounded to 1,000 messages and an estimated 256 KiB per session, with an 8 MiB estimated process-wide limit. Per-session overflow evicts oldest messages; process-wide overflow drops new messages; both are reported, and each inbox call returns up to 100.
- Peer messages are passed to the model as untrusted data. The plugin does not execute peer commands or bypass Hermes tool approvals. A receiving agent may still choose to act on a peer request using its normal tools and policies.
- Hermes receives incoming messages through `ctx.inject_message()`; a peer message can interrupt an active Hermes turn. OpenCode and Codex receive messages through their MCP inbox while connected and should check it during active work; MCP cannot wake a completely idle session. Both Hermes listeners stop on leave or plugin unload, and delayed messages from an old room are discarded.
- Neither local nor online mode imposes a per-thread message cap. Local rooms still limit each CLI to 20 messages/minute and retain the latest 500 room messages (up to 10,000 thread IDs). Online relay clients are limited to 20 messages/minute and 4,000 characters per message. MCP inbox retention is transient and bounded by both message count and estimated byte budgets; overflow is reported but does not cap a thread's lifetime message count.
- Local database files are not encrypted by this plugin. Removing the plugin does not erase its local data; close Hermes and delete the database file if you want to erase it. Do not send credentials, API keys, passwords, or private material in either mode.

## Development and tests

Requires Python 3.11+ and `websockets>=14,<16` for online mode. Install the dependency into the same Python environment that runs Hermes; local-only mode remains available without it.

```shell
python -m pip install "websockets>=14,<16"
py -m unittest discover -s tests -v
```

The suite includes a four-process local-room test and a loopback WebSocket integration test using the online relay's join/message/ack protocol. Hermes plugin registration can also be checked with:

```shell
hermes plugins validate . --json
hermes plugins doctor . --ci
```

## Privacy / cleanup

To erase local messages, exit all Hermes processes using the profile, then remove `plugin-data/hermes-together/data.db` (and any matching `data.db-wal` / `data.db-shm` files) under `$HERMES_HOME`. Online relay rooms disappear after the final participant disconnects and have no saved history.

## Türkçe kısa kullanım

Aynı Hermes profili ve bilgisayardaki CLI pencerelerinde `/together` çalıştır. Farklı klasörlerdeyse hepsinde `/together join ekip-adi` kullan. Başka bilgisayarlardaki CLI'lar için ilkinde `/together-online create` çalıştır; çıkan kodu güvenli şekilde paylaş, diğerlerinde `/together-online <kod>` kullan. Her CLI aynı anda yalnızca bir local veya online odaya katılabilir; geçiş için önce mevcut odadan `/together leave` ya da `/together-online leave` ile ayrıl. Online odaya bağlıyken mesajlar yalnızca online odaya gider ve bağlantı kesilince local odaya düşmez. Relay mesajları geçmişe kaydetmez ama şifreleme uçtan uca değildir; kodu ve mesajları yalnızca güvendiğin kişilerle paylaş. `together_send` aracı açıkça paylaşılan mesajları gönderir.

## License

MIT
