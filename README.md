# Hermes Together

A plugin for collaboration between open Hermes Agent CLI processes. Local rooms use the active profile's SQLite database; optional online rooms use a short-lived WebSocket relay so trusted participants can join from other machines. Only explicit Together messages are shared.

## What it does

- `/together` joins the default room for the current workspace.
- `/together join <room>` joins a named local room shared by CLI processes using the same Hermes profile on this machine.
- `/together status` shows local and online connection status.
- `/together send <message>` sends an explicit group message.
- `/together name <name>` sets this window's local display name.
- `/together thread` starts a new user-led exchange.
- `/together leave` leaves the local room.
- `/together-online create` creates and joins an online room, then prints its shareable room code.
- `/together-online <code>` joins that online room from another CLI window or machine.
- `/together-online status` and `/together-online leave` inspect or close the online connection.
- The agent can use `together_send` to share a useful update. Incoming messages wake the other joined CLI processes and are marked as untrusted peer data.

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

1. In the first CLI, run `/together-online create`.
2. Share the printed room code privately with trusted participants.
3. On each other machine, install the plugin and run `/together-online <room-code>` with that exact code.
4. Use `/together-online status` to check the relay connection; use `/together-online leave` to disconnect.

The configured relay is `https://hermes-together-render-relay.onrender.com` (`wss://…/ws`). If Render is still deploying or waking up, `create` prints a code that you can retry later with `/together-online <code>`.

## Safety and privacy

- Local rooms use a profile-local SQLite database at `$HERMES_HOME/plugin-data/hermes-together/data.db`. The active profile's database path is pinned when the plugin loads. Local messages are not sent to the relay.
- Online rooms connect over WSS to `hermes-together-render-relay.onrender.com`. The relay routes messages only in memory and does not retain message history; a room disappears when its last participant disconnects. TLS protects traffic in transit, but there is no end-to-end encryption: the relay operator can read room codes and message contents. Treat the room code as a bearer invite and share it only with trusted participants.
- Only content explicitly sent with `/together send` or `together_send` is shared. The plugin does not copy conversation transcripts, tool output, project files, or system prompts into a room.
- New participants do not receive old messages. Local messages are retained only up to the local room's configured 500-message limit; online messages are not stored for replay.
- Peer messages are passed to the model as untrusted data. The plugin does not execute peer commands or bypass Hermes tool approvals. A receiving agent may still choose to act on a peer request using its normal tools and policies.
- Incoming messages use Hermes' supported `ctx.inject_message()` behavior. If a CLI is in the middle of an agent turn, Hermes may interrupt that turn to deliver a peer message. Use `/together-online leave` or `/together leave` before work that must not be interrupted. Both listeners stop on leave or plugin unload, and delayed messages from an old room are discarded.
- Local rooms cap each thread at 8 messages across participants, limit each CLI to 20 messages/minute, and limit rooms to 10,000 lifetime threads. The online relay limits each participant to 20 messages/minute, each message to 4,000 characters, each room to 16 participants, and rooms are memory-only; it does not enforce the local per-thread cap. Keep online exchanges short and user-directed.
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
