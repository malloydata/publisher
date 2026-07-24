# {{title}}

This directory is a [Malloy Publisher](https://github.com/malloydata/publisher)
workspace. It serves Malloy semantic models over a web UI, a REST API, and an MCP
endpoint. If you are an AI agent working here, this is what you can do and how to
start.

## Start the server first

Everything below talks to a running server, so start it before anything else:

```bash
{{startCommand}}
```

That is meant to run Publisher on http://localhost:{{port}} (web UI and REST) with the
MCP endpoint on http://localhost:{{mcpPort}}/mcp, and to mount this workspace's
packages in watch mode so model edits recompile. All three of those come from flags in
the command above (`--port`, `--mcp_port`, `--watch-env`), and where that command is an
`npm` script, from the script `package.json` runs for it. Read them there before you
trust anything below: different ports move every URL in this file, a `--server_root` or
`--config` pointing outside this directory serves a different workspace entirely, and
no `--watch-env` means an edit on disk is not picked up at all until you reload the
package. The server settles all of it on boot, printing
`Publisher server listening at http://<address>:<port>` and a
`MCP server listening at ...` line beside it, so read those two lines rather than
taking this paragraph's word for it.

Where watch mode is on, a recompile that *fails* fails quietly, and not always in the
same way. It may leave the last version that compiled serving, so a query keeps
returning the old model's rows; it may instead make the source vanish, so every query
fails with `Reference to undefined object '<source>'`. Either way the failure goes only
to the server's stdout, which you are not reading if you started it in the background.
So a query that succeeds after an edit is not proof the edit compiled, and the rows you
get back may be the old model's. Compile-check the edit with `malloy_compile`, or
reload with `malloy_reloadPackage` afterwards (both described below); either one
reports the failure that watch mode swallowed.

Poll until it reports serving rather than assuming a fixed wait; the first run
downloads the server, so it can take a minute:

```bash
curl -s http://localhost:{{port}}/api/v0/status
```

Read two things out of that response before you trust it, because any Publisher
holding this port answers it the same way:

- `operationalState` is `serving`, and
- the environment's `location` is a path inside this workspace directory, and its
  `packages` list contains the package you are working on.

If the location points somewhere else, the ports were already taken. {{port}} and
{{mcpPort}} are Publisher's defaults, so another workspace's server may hold them.
The server exits when it cannot bind, but the last lines on screen are the green
`Service marked as ready` and `Environment store successfully initialized` from the
startup work that did finish; the `EADDRINUSE` line is further up. Both workspaces
also name their environment `default`, so nothing in the response looks wrong except
that path. Scroll up for `EADDRINUSE`, then either stop the other server or move
this one onto free ports:

```bash
{{portOverrideCommand}}
```

Every URL below then uses the port from that command in place of {{port}}. The MCP
endpoint moves with it, which also means editing the `url` in this workspace's MCP
config and reconnecting, so prefer stopping the other server when you can. That second
pair of ports is another guess at a free pair, not a checked answer, so run the same
`location` and `packages` check above against the new port before you trust it.

The two flags belong to that one boot and nothing remembers them. Every later boot of
this workspace needs them spelled out again, in the same shape as the command above:
that includes the reset command further down, which without them goes back to
{{port}} and {{mcpPort}} and collides with the same server all over again.

New ports only help against a server on a *different* workspace. Two servers cannot
share this one: the second binds its free ports and then sits at `initializing`
forever, because the first holds the lock on `publisher.db`. One server per
workspace, always.

Before you trust the endpoint to be private, read the `--host` in the command you
are actually running, which is the start block above (and, where that block is an
`npm` script, the script `package.json` runs for it). Neither the REST API nor the
MCP endpoint authenticates anything, so that bind address is the only thing between
this data and the rest of the network. `--host 127.0.0.1`, written with a space
before the address, binds loopback: this machine and nothing else. Publisher's own
default is `0.0.0.0`, so a command with no `--host` at all, or one that names
`0.0.0.0` or a LAN address, is serving every machine on the network. To do that
deliberately, put something that authenticates in front of it first.

Then check that command against what the server itself reports, because the command
is not the last word. On boot it prints
`Publisher server listening at http://<address>:<port>` and
`MCP server listening at ...`, and those two lines are the addresses it really bound.
An unknown flag is accepted without a word of complaint, so a mistyped `--hostt` is
dropped in silence and the bind falls back to Publisher's own `0.0.0.0` while the
command still reads as loopback. `--host=127.0.0.1` fails in exactly that way and is
the easier one to write by accident: Publisher reads `--host` and its address as two
separate arguments, so the joined `=` form matches no flag it knows, is dropped
without a word, and the bind falls back to `0.0.0.0`. It is the same log you scroll
through hunting `EADDRINUSE`, so read the listening lines while you are in there.

One more restart trap: if a package is added to `publisher.config.json` after the
server has booted once, a plain restart will not serve it. Publisher reads that file
only while `publisher_data/` holds no database; from the second boot on its own
database wins, so the new package is registered, reported by the scaffolder, and
never mounted, with nothing in the log to say why. Boot once with the reset command,
which is the start command above plus `--init`, to rebuild the persisted state from
the config, then go back to the normal start:

```bash
{{resetCommand}}
```

Stop the running server first. It wants the ports and the `publisher.db` the live
one already holds, so with the server up it fails twice over: `EADDRINUSE` on both
ports, then `Could not set lock on file "publisher.db"` (which names the PID holding
it). If you moved the ports above, add the same `--port` and `--mcp_port` flags to
this command as well, spelled exactly the way the port-override command above spells
them, `--` separator and all where that command carries one; nothing carries them
over for you. Note also that `--init` clears persisted runtime state, so materialized
tables and saved themes go with it; it rebuilds from `publisher.config.json`, never
from the database it replaces.
{{packageSection}}
## Query the data (MCP or REST)

Two interfaces reach the same models with the same governance.

MCP, for an interactive agent: the tools are `malloy_getContext` (discover
environments, packages, sources, and fields), `malloy_executeQuery` (run a view or
ad-hoc Malloy and get JSON back), `malloy_compile` (compile-check a snippet of Malloy
against the model, so it takes the snippet as a required `source` argument; it reports
the model file's own errors too),
`malloy_reloadPackage` (pick up on-disk model edits with no restart, and surface a
watch-mode recompile that failed), and
`malloy_searchDocs`. {{mcpNote}}

REST, for a script or a check that does not need an agent: every model is queryable
at `POST /api/v0/environments/<env>/packages/<package>/models/<model>/query`. This
lists what the server has loaded:

```bash
curl -s http://localhost:{{port}}/api/v0/environments/{{envName}}/packages
```

That list is what this workspace serves. This file is not: it describes at most one
package, the one the scaffolder created on the run that wrote this file. A workspace
scaffolded more than once serves more than that, so take the packages, sources and
views you do not see here from the list above and from `malloy_getContext`, never
from their absence here.

If you started this server yourself in this session, your `malloy_*` tools will not
appear however long you wait: an MCP client fixes its tool list when it connects, so
it never saw a server that did not yet exist. You cannot reconnect yourself. Say so,
and {{reconnectNote}} Do not quietly switch to curl instead and call it done: it
looks like it is working while hiding a problem the user can clear in seconds, and
it gives up the grounded discovery, compile checks, and reload the tools exist to
provide. If the user would rather keep going without reconnecting, the REST endpoint
above runs the same models.

## Skills ({{skillsCount}} installed)

{{skillsNote}}

The names below are that guidance broken out by task. Load them as installed skills
where the count above says they are here, and pull the same names as MCP prompts from
the endpoint above where it says they are not.

Start with `malloy-getting-started`. Use `malloy-modeling` to build or change the
model, `malloy-analysis` to answer questions, and `malloy-review` to check Malloy for
correctness.

Read the gotchas before you write, not after you fail: `malloy-gotchas-modeling` for
sources, dimensions, measures and joins, `malloy-gotchas-queries` for views and
queries, and `malloy-gotchas-rendering` for chart and formatting tags. They hold the
traps that cost the most time on a first model, including the two that a column of
real data usually springs:

- A column named `date`, `hour`, `number`, `source`, `type`, `count` or another
  Malloy reserved word has to be backticked, or it fails with cascading errors on
  lines that look unrelated.
- `avg(score::number)`, which `malloy-modeling` gives as a quick reminder, is only
  right on a clean column. `::number` is a strict cast, so a column carrying `'NA'`,
  `'N/A'`, `''` or `'-'` compiles and then throws at query time
  (`Could not convert string 'NA' to DOUBLE`). The fix is
  `avg(nullif(score, 'NA')::number)`. Where the two skills disagree,
  `malloy-gotchas-modeling` is the correct one.

When something does not compile, reach for `malloy-debug` rather than guessing at the
error: it covers reading Malloy's messages, and why fixing the first error usually
clears the rest of a 20-error cascade.
