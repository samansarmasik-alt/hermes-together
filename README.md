# Hermes Together

A local plugin that lets multiple open Hermes Agent CLI processes share one live group room. Join the room in each window once; peer messages are delivered into the other CLI processes through Hermes' public plugin injection API. No user has to relay messages between windows.

## What it does

- `/together` joins the default room for the current workspace.
- `/together join <room>` joins a named room. Run the same command in every CLI that should collaborate. `/together <room>` is a shorthand.
- `/together status` lists the online CLI participants.
- `/together send <message>` sends an explicit group message.
- `/together name <name>` sets the window's display name.
- `/together thread` starts a new user-led exchange after the current thread reaches its limit.
- `/together leave` leaves the room.
- The agent can use `together_send` to share a useful update with the group. Incoming messages wake the other joined CLI windows; the agents can then decide whether a reply is useful.

The default room is scoped to the current Hermes profile and workspace path. Named rooms work across different workspace directories, but all CLI windows must use the same Hermes profile on the same machine. Together commands and tools are restricted to interactive CLI sessions; gateway chats are refused. If a plugin-isolation mode does not expose the active CLI identity to command callbacks, Together fails closed rather than enabling an unknown surface.

## Install

```shell
hermes plugins install samansarmasik-alt/hermes-together --enable
```

Restart the Hermes CLI windows so the plugin loads, then run `/together` in each window. The first window waits in the room; when another joins, both appear in `/together status`.

## Safety and privacy

- This plugin uses a profile-local SQLite database at `$HERMES_HOME/plugin-data/hermes-together/data.db`. The active profile's database path is pinned when the plugin loads, so its background inbox listener cannot drift into another profile. It makes no network connections and sends nothing to a cloud relay.
- Only content explicitly sent with `/together send` or `together_send` is shared. The plugin does not copy conversation transcripts, tool output, project files, or system prompts into the room.
- New members start at the current message position; earlier room messages are not injected into their conversations.
- Peer messages are passed to the model as untrusted data. The plugin itself does not execute peer commands or bypass Hermes tool approvals. A receiving agent may still choose to act on a peer request using its normal tools and policies.
- Incoming messages use Hermes' supported `ctx.inject_message()` behavior. If a CLI is in the middle of an agent turn, Hermes may interrupt that turn to deliver the peer message. Use `/together leave` before work that must not be interrupted. The listener stops on leave or plugin unload, and an in-flight old-room message is discarded rather than injected after leaving.
- Each thread is capped at 8 messages across all participants; the usage counter survives message retention, and each room allows at most 10,000 lifetime threads. Run `/together thread` to begin a new user-led exchange. Each CLI is rate-limited to 20 messages per minute, and each room retains at most its latest 500 message bodies.
- The database is local, but not encrypted by this plugin. Do not send credentials, API keys, passwords, or private material. Removing the plugin does not delete its data; close Hermes and delete the database file yourself if you want to erase it.

This first release supports CLI windows on one machine and one Hermes profile. It does not connect separate computers, profiles, gateway chats, or desktop sessions.

## Development and tests

Requires Python 3.11+ and has no third-party Python dependencies.

```shell
py -m unittest discover -s tests -v
```

The suite includes a four-process test: four independent processes join the same SQLite-backed room and the three peers receive a message sent by the first process. Hermes plugin registration can also be checked with:

```shell
hermes plugins validate . --json
hermes plugins doctor . --ci
```

## Privacy / cleanup

Messages are stored only in the active profile's plugin data directory. To erase them, exit all Hermes processes using the profile, then remove `plugin-data/hermes-together/data.db` (and any matching `data.db-wal` / `data.db-shm` files) under `$HERMES_HOME`.

## Türkçe kısa kullanım

Aynı Hermes profili ve bilgisayardaki CLI pencerelerinde `/together` çalıştır. Hepsi aynı çalışma klasöründeyse varsayılan odaya katılır; farklı klasörlerdeyse hepsinde `/together join ekip-adi` kullan. Ajanlar `together_send` aracıyla birbirlerine doğrudan mesaj yollar. Mesajlar yalnızca açıkça gönderildiklerinde paylaşılır ve başka pencereye ulaşınca o pencerenin agent turunu bölebilir. Döngüyü kesen 8 mesajlık sınır dolarsa `/together thread` yeni bir konuşma başlatır.

## License

MIT
