# create-malloy-package

Scaffold a [Malloy Publisher](https://github.com/malloydata/publisher) package and a
local agent workspace in one command. You get a working semantic model over sample
data, a server config, and a pre-wired agent environment (MCP connection, agent
instructions, and the Malloy agent skills copied in), so you can go from nothing to
"an agent can query my data" without hand-editing anything.

## Quick start

```bash
mkdir my-data && cd my-data
npm create malloy-package sales
npm start
```

The workspace files land in the current directory and the package in `./sales`, so you
run `npm start` from where you created it (no need to `cd` into the package). The
scaffolder's own last lines are the ones to keep: the app URL, the MCP endpoint, and a
`curl` that tells you when the server is ready. `npm start` itself boots Publisher with
the package mounted in watch mode and then streams the server's log, which reports the
address it bound to (`http://127.0.0.1:4000`) and never says "ready" in so many words.
Run the readiness check the scaffolder printed:

```bash
curl -s http://localhost:4000/api/v0/status
```

It answers `"operationalState":"serving"` once the server is up. On a first run that
includes downloading the server, so it can take a minute; poll rather than guessing a
wait.

`serving` describes the server, not your package, so follow it with the question that
is actually about your data:

```bash
curl -s http://localhost:4000/api/v0/environments/default/packages
```

A package that failed to load is missing from that list, and an empty `[]` means
nothing loaded at all while the status endpoint still says `serving`. A data file
DuckDB cannot read is the usual cause; the server log carries the reason on an error
line reading `Failed to load package <name>`.

`default` is the environment name in a fresh workspace. If the scaffolder added your
package to a `publisher.config.json` that was already there, the environment is
whatever that file names, and the generated agent briefing spells out the URLs for
this workspace. That briefing is `AGENTS.md`, or `AGENTS.malloy.md` in a directory
that already had an `AGENTS.md` of its own; see "Running it again" below.

## Query it

The web UI at http://localhost:4000 is the quickest look. For a check you can script,
every model is queryable over REST at
`POST /api/v0/environments/<env>/packages/<package>/models/<model>/query`. Run one of
the starter model's views by name:

```bash
curl -s -X POST \
  http://localhost:4000/api/v0/environments/default/packages/sales/models/sales.malloy/query \
  -H 'Content-Type: application/json' \
  -d '{"sourceName": "sales", "queryName": "by_category", "compactJson": true}' \
  | jq -r .result
```

```json
[{"category":"Furniture","total_amount":789},{"category":"Electronics","total_amount":489.95},{"category":"Apparel","total_amount":375}]
```

The source is named after the package, and the sample model ships the views
`by_category`, `by_region`, `sales_by_month` and `overview`. A package seeded with
`--data` starts with `overview` alone, since the scaffolder does not know your columns.
The model in the URL is the file inside the package. To send Malloy rather than name a
view, swap the `-d` payload for one that carries a query and no `sourceName`:

```bash
  -d '{"query": "run: sales -> by_category", "compactJson": true}'
```

The response is `{"result": "<rows as a JSON string>", "resource": "..."}`, hence the
`jq -r .result` above. Leave `compactJson` off to get the full Malloy result with the
type metadata the renderer uses.

It has to be a POST, and the path has to be right: any `/api/v0/...` URL that matches
no route falls through to the web app and answers `200` with an HTML page, which reads
as success until you look at the body.

Agents should reach the same models through MCP rather than curl, which buys them
schema discovery, compile checks and a reload that needs no restart. The generated
briefing lists those tools.

## What it creates

Running `npm create malloy-package sales` in an empty directory produces:

```
publisher.config.json    the server config, with your package registered
package.json             "npm start" runs Publisher in watch mode, "npm run reset"
                         does the same but rebuilds state from the config first
.gitignore               ignores node_modules/, publisher_data/, publisher.db*,
                         *.log and .DS_Store
.mcp.json                a pre-wired MCP connection to the local server
CLAUDE.md / AGENTS.md    short, package-scoped agent instructions
.claude/skills/          the Malloy agent skills, copied in as real files
sales/                   the package
  publisher.json         the manifest
  sales.malloy           a starter model over the sample data
  data/sales.csv         the sample data
```

The `publisher.db*` entry is a glob rather than a plain `publisher.db` on purpose:
DuckDB leaves a `.wal` sidecar behind even after a clean shutdown, so without the glob
a `git add -A` would stage the server's database state.

### Running it again

If the workspace files already exist (you ran it once before), a second run extends
them instead of overwriting them:

- the new package is added to the existing `publisher.config.json`, keeping the
  environments, connections, and packages already registered there;
- the `malloy` server is merged into the existing MCP config (`.mcp.json`, or
  `.cursor/mcp.json` for Cursor) alongside every other server in it, so a hand-edited
  MCP config keeps the servers you put there;
- an existing `.gitignore` is appended to, never rewritten: it gains only the entries
  above that are not in it already, keeps every rule of yours byte for byte, and is
  listed under "Wrote these" as `.gitignore (n entries appended, nothing removed)`;
- the files it will not rewrite (`package.json`, `CLAUDE.md`, and an `AGENTS.md` this
  tool did not write) are left as they are and listed under "Left these existing files
  alone" in the output, so the run reports what it did not do as well as what it did.

The agent briefing is the exception to that last bullet, and has a section of its own
below.

`--force` replaces `CLAUDE.md`, and an `AGENTS.md` that is yours rather than this
tool's, outright, so move anything of yours out of them first. The other three files
are merges, not rewrites, because most of what is in them is yours:

- `.gitignore`: appended to, with or without `--force`. Appending is idempotent, so
  `--force` changes nothing here and there is nothing of yours to move out first.
- `package.json`: `--force` sets the `start` and `reset` scripts and keeps the rest of
  the manifest. Under `--force`, a `package.json` that cannot be read, or that is not
  valid JSON, or that holds something other than a JSON object, stops the run before
  anything is written at all, rather than being replaced with a stub. Without
  `--force` the manifest is never written to in the first place, so a file in any of
  those states is left alone and listed with the reason, and the run prints the full
  `npx @malloy-publisher/server ...` command to use instead of an `npm` script.
- the MCP config (`.mcp.json`, or `.cursor/mcp.json` for Cursor): only the `malloy` key
  is this tool's, so every other server in the file survives with or without `--force`.

An MCP config the tool cannot read as a JSON object is the one it cannot merge into: a
syntax error, or an `mcpServers` that is not an object. `--force` does not change that.
The file is left byte for byte as it was, listed under "Left these existing files
alone" with the reason, and the `malloy` server block is printed for you to paste in by
hand. Paste it, or fix the file and run again.

**Restart the second package with `npm run reset`, not `npm start`.** Registering the
package in `publisher.config.json` is not enough on its own once the server has run:
Publisher reads that file only while `publisher_data/` holds no database, and from the
second boot on its own persisted state wins. A package added later is therefore
registered, reported as created, and never served, with nothing in the server log to
say why. `npm run reset` is the same boot with `--init`, which rebuilds the persisted
state from the config; after it has served the new package once, `npm start` is enough
again. That holds for the `reset` script this tool writes. Where the scaffolder left an
existing `package.json` alone, a `reset` script already in it is not this tool's and
need not carry `--init` at all, so use the printed `npx @malloy-publisher/server ...`
command with `--init` on the end instead. `--init` is deliberately not part of `start`,
because it also clears runtime
state (materialized tables, saved theme) on every boot. If you moved the ports, pass
them here too (`npm run reset -- --port <p> --mcp_port <m>`); see "Ports and network
exposure" below.

**Stop the running server before you reset.** A workspace holds one DuckDB database and
one lock on it, so a second server over the same directory cannot start even on
different ports. It prints
`Could not set lock on file "publisher.db": Conflicting lock is held ... (PID nnnn)`,
then sits at `"operationalState":"initializing"` and stays there rather than exiting.
That covers `npm run reset` while `npm start` is up, and any second server pointed at
this same workspace. Two workspaces in two directories are fine, as long as they are
not on the same ports.

If the scaffolder left an existing `package.json` alone, it added no `start` or `reset`
script of its own. That manifest may well have scripts by those names already, but they
boot whatever their author meant them to boot: not necessarily Publisher, not
necessarily this workspace, not necessarily on loopback, and in the `reset` case not
necessarily with `--init`. So the run does not recommend them blind. It prints the full
`npx @malloy-publisher/server ...` command to run instead, and says that no Publisher
`start` (or `reset`) script was added here; the reset is that same command with `--init`
on the end. Where it read enough of the existing script to say something specific, it
gives the reason as well: that the script boots Publisher with no `--host`, or writes
the `--host=` form Publisher silently drops, or binds a non-loopback address, or names
the server inside a longer command line, or in the `reset` case carries no `--init`. A
script that boots something else entirely (a bundler, a test runner) gets the general
sentence rather than a named reason, because nothing here parsed what it runs.

#### The agent briefing, and `AGENTS.malloy.md`

The briefing is the one file that is regenerated rather than merged or skipped. It is
written in full from the run's own state (the start command, the ports, the package,
the skills count), so there is nothing in it to preserve, and a copy left describing
the workspace as it was two packages ago is the failure it exists to prevent.

- In a workspace this tool scaffolded, the briefing is `AGENTS.md`, and every later
  run rewrites it. It is listed under "Overwrote these", alongside `.claude/skills/`,
  which is refreshed the same way and for the same reason.
- In a directory that already had an `AGENTS.md` of its own, that file is not this
  tool's to touch and is left byte for byte as it was. The briefing goes beside it as
  **`AGENTS.malloy.md`**, and the `CLAUDE.md` the run writes points there instead, as
  does the run's closing line. `AGENTS.malloy.md` is this tool's file, so later runs
  rewrite it too.

Either way the briefing describes one package: the one that run created. Scaffold a
second package into the same workspace and the briefing is regenerated about the
second one, while the server goes on serving both. The list of everything the
workspace actually serves is the packages endpoint:

```bash
curl -s http://localhost:4000/api/v0/environments/default/packages
```

## Ports and network exposure

The server listens on 4000 (web UI and REST) and 4040 (MCP), Publisher's defaults.
Pass `--port` and `--mcp_port` to move them, with npm's `--` separator in front:

```bash
npm start -- --port 4100 --mcp_port 4140
```

The flags belong to that one boot and nothing remembers them, so every later boot needs
them again, `npm run reset` included. A bare `npm run reset` goes back to 4000 and 4040
and collides with whatever was in the way the first time:

```bash
npm run reset -- --port 4100 --mcp_port 4140
```

4100 and 4140 are a guess at a free pair, not a checked answer. Run the readiness check
against the new port and read `location` and `packages` out of it, exactly as you would
on 4000.

Moving the MCP port also means changing the `url` in `.mcp.json` (or
`.cursor/mcp.json`) to match, since that file was written for 4040.

**A port collision looks like success.** If another Publisher already holds 4000 and
4040, the second one fails to bind and exits 1, but the last lines it prints are the
green `Service marked as ready` and `Environment store successfully initialized` from
the startup work that did finish; the `EADDRINUSE` error is well above them. The
readiness check then answers `"operationalState":"serving"` from the *other* server,
and because both workspaces name their environment `default`, the only tell is the
`location` and `packages` in the response describing a different directory. Check
those two fields, or move this server onto free ports.

Moving ports only helps when the server in the way is serving a different workspace. If
it is this workspace's own server, free ports change nothing: the second one takes the
database lock error above and never reaches `serving`. Stop the first server instead.

The `start` script this tool writes passes `--host 127.0.0.1`, so the workspace is
reachable from your machine only. Publisher's own default is `0.0.0.0`, which suits a
deployed server; here neither the REST API nor the MCP endpoint has any authentication
in front of it, so binding to every interface would hand the local network a read of
your data. To expose it deliberately, change `--host` in the `start` script to `0.0.0.0`
(or to a specific interface) and put something that authenticates in front of it.

All of that describes the script this tool writes. In a directory that already had a
`package.json`, the scaffolder leaves it alone (see "Running it again"), so `npm start`
runs whatever `start` was already there, binding wherever that command says. The same
goes for everything else on this page that reads off a flag: `--port` and `--mcp_port`
decide the URLs, `--server_root` and `--config` decide which workspace is served at
all, and `--watch-env` decides whether an edit on disk is picked up without a reload.
Read them out of the command you are about to run rather than off this page.

Then read it off the server, which is the only ground truth. On boot it prints
`Publisher server listening at http://127.0.0.1:4000` and an `MCP server listening at`
line beside it, and those are the addresses it really bound. Publisher accepts an
unknown flag without complaint, so a mistyped `--hostt` is dropped in silence and the
bind falls back to the `0.0.0.0` default while the command still reads as loopback. The
`=` form goes the same way: Publisher reads `--host` and its address as two separate
arguments, so `--host=127.0.0.1` matches no flag it knows, is dropped without a word,
and binds `0.0.0.0`. Write the flag and the address with a space between them.

## Options

```bash
npm create malloy-package [name] -- [options]
```

`npm create` parses the command line with npm's own config parser before handing
what is left to the scaffolder, so flags must sit behind a `--` separator. Without
it npm swallows `--force` as one of its own settings and turns `--data mydata.csv`
into two stray positional arguments:

```bash
npm create malloy-package sales -- --data mydata.csv
npm create malloy-package sales -- --client cursor
npm create malloy-package sales -- --force
```

Running the published bin directly takes the flags as-is, with no separator:

```bash
npx create-malloy-package sales --data mydata.csv
```

- `name` (positional): the package name. Omit it to only set up the agent workspace in
  the current directory (write the MCP connection, agent instructions, and skills)
  without scaffolding a package.
- `--data <file>`: seed the package from your own CSV or Parquet file instead of the
  built-in sample. The file is copied into the package and the starter model points at
  it. It seeds a new package, so it requires a package name: it cannot be combined with
  the setup-only mode above, and passing it without a name is an error rather than a
  silently ignored flag.
- `--client <claude-code|cursor>`: which agent client to wire up. Defaults to
  `claude-code`. `AGENTS.md` and the skills in `.claude/skills/` are written for every
  client; the MCP config file (`.mcp.json` for Claude Code, `.cursor/mcp.json` for
  Cursor) and `CLAUDE.md` are the client-specific parts. The flag is `--client` rather
  than `--host` because the generated start command passes Publisher's own `--host`
  for the bind address, and two meanings of one flag name in a single workspace is a
  trap.
- `--force`: overwrite existing workspace files instead of extending them. The `--`
  separator matters most here: npm has a `--force` of its own, so a bare one never
  reaches the scaffolder.

## Requirements

Node 20 or newer. The generated `npm start` uses `npx` to fetch the Publisher server on
first run, so no global install is needed. The default template runs entirely on the
package's built-in DuckDB sandbox, so no database credentials are required.

### The workspace path

Create the workspace somewhere whose full path is made only of letters, digits, `-`,
`_`, `.` and `/`. DuckDB cannot read a data file under a path containing a space, a
parenthesis, an apostrophe, or any non-ASCII character, and Publisher resolves the
model's relative table path against the workspace directory before that check runs. So
a single space anywhere above the package makes every model in it fail to load, with
the server still reporting healthy and the only visible symptom an empty package list.

Common directories that trip it: `~/Documents/My Projects`, `~/Google Drive`,
`~/OneDrive - Company`, and any home directory whose username carries an accent. The
scaffolder checks this before writing anything and refuses to run in such a directory,
naming the offending character. Move to a path like `~/malloy-workspace` and run again.

## License

MIT
