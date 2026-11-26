<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# The package format

> What this is: what a Malloy Publisher package is on disk. The files, the `publisher.json`
> manifest fields, where a package's data comes from, and how a package gets served. For the
> server's own config file, see [configuration.md](configuration.md); for running a server, see
> [deployment.md](deployment.md).

## A package is a directory with two files

A package is a directory holding a `publisher.json` manifest and at least one `.malloy` model.
That is the whole format. Data files sit alongside the models:

```
sales/
  publisher.json        # the manifest ({} is valid)
  sales.malloy          # a Malloy model
  data/
    sales.csv           # data the model reads through the built-in duckdb connection
```

A working minimal pair:

```json
{ "name": "sales", "description": "Sales by region" }
```

```malloy
source: sales is duckdb.table('data/sales.csv') extend {
  measure: total_rows is count()

  view: by_region is {
    group_by: region
    aggregate: total_rows
  }
}
```

The manifest must exist and parse as JSON: a directory without a `publisher.json` is not a
package, and a manifest that fails to parse fails the package load. Beyond that, every field is
optional; `{}` is a valid manifest.

One filename is special. If the package root holds an `index.malloy`, that file is the package's
published surface: listings return it and whatever it `export`s, the other models become building
blocks, and a direct query against a source it does not export is refused. It is the
no-configuration way to curate a package, and it stays entirely optional. See
[discovery-and-access.md](discovery-and-access.md).

## The manifest: publisher.json

The fields the server reads:

| Field              | Purpose                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`             | Conventionally the package's name, but the server never surfaces it: the registered name (config entry or API call) wins in API URLs and responses.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `description`      | Shown in the Publisher UI and in API responses.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `explores`         | **Deprecated, still honored.** Names the files whose exports are the surface; those files are listed and queryable. A tagged report listed here reads the surface and adds nothing to it. Every use gets a load warning: an `index.malloy` that imports those files replaces it. Reports never need to be listed. See [discovery-and-access.md](discovery-and-access.md).                                                                                                                                                                                                                                                                                                                                     |
| `queryableSources` | `"declared"` (the default) or `"all"`. Deprecated. `"declared"` does nothing, and writing it gets a warning. `"all"` still works with no warning, because nothing replaces it: the surface then decides listings only, and every source stays queryable by name. Use it to hide an `#(authorize)`-gated source from listings while authorized callers still query it. See [discovery-and-access.md](discovery-and-access.md).                                                                                                                                                                                                                                                                                 |
| `materialization`  | Persisted-source build policy (`schedule`, `freshness`, `scope`). Package root only. Its `queryMetadata` is deprecated; declare `queryMetadata` at the root. See [materialization.md](materialization.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `scope`            | Deprecated at the root: declare it as `materialization.scope`. `"package"` (the default) or `"version"`. Any other value fails the package load.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `workflows`        | Named workflows the package declares, keyed by workflow name: instructions and skill sets. Its `default` workflow holds the package's own skills. See [Workflows](#workflows). |
| `retrieval`        | How `get_context` searches and indexes this package: `fcj` (the engine's settings, such as `perTarget` and `judge`), `sourceSummary` (`{ "enabled": "auto" \| true \| false }`), and `prompts` (a file path inside the package for each of `judge`, `docParts`, `questions`, `sourceSummary`). Any other key under `retrieval`, or an invalid value, fails the package load, except `engine`, `representation`, `keyphrases`, `refine`, `rerank`, `sourceMatch` and the prompts `keyphrase`, `refine`, `rerank` and `sourceMatch`: those no longer do anything, are ignored, and are named in the package's `warnings`. See [configuration.md](configuration.md#package-settings-retrieval-in-publisherjson). |

Unknown top-level keys are ignored and preserved. Inside `retrieval` they are not: an unknown key fails the
package load with a message naming the valid ones. `version` is read when the package is published
from a location (`POST …/packages` with a `location`): it numbers the immutable version that publish
creates, and a publish without one is refused. A package in `publisher.config.json` is published
the same way, unless its environment is watched (`--watch-env`), which mounts it in place and
ignores it. See [package-versions.md](package-versions.md). (One more field, `manifestLocation`, exists for orchestrated
control-plane deployments; a locally authored package never needs it. It is deprecated and goes
away with the package `PATCH` that writes it: bind a version's build manifest with
`PUT …/versions/{v}/manifest` instead. See [#1540](https://github.com/malloydata/publisher/issues/1540).)

<a id="agents"></a>

## Workflows

A package can declare workflows: named jobs its author wrote for working with this package. A workflow is optional instructions (a Markdown brief) and skill sets (folders of skills to use). Publisher parses and serves the definition. It does not run the workflow. A client adopts one by fetching it (`get_workflow`, or `GET …/packages/{pkg}/workflows/{name}`) and following it in its own session, with its own tools and permissions; the bundled `malloy-run-workflow` skill is the recipe, and `malloy-define-workflow` covers writing a definition. Adopt a workflow only when the user names it: nothing advertises one in discovery. The exception is the `default` workflow, whose skills are the package's own skills ([below](#the-default-workflow)).

```json
{
  "name": "storefront",
  "workflows": {
    "default": {
      "skill_sets": ["workflows/default/skill_sets/house"]
    },
    "weekly-report": {
      "description": "Writes the weekly storefront report as the analyst",
      "instructions": "workflows/weekly-report/instructions.md",
      "skill_sets": [
        "workflows/weekly-report/skill_sets/analyst",
        "workflows/default/skill_sets/house"
      ]
    }
  }
}
```

| Key            | Meaning                                                                                                                                                                              |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `description`  | Optional. A non-empty line saying what the workflow is for.                                                                                                                          |
| `instructions` | Optional. Package-relative path of a `.md` file: the brief the workflow works from. At most 16 KiB.                                                                                  |
| `skill_sets`   | Optional. At most 8 package-relative folders, each a [skill set](#skill-sets). The order decides which set serves a skill name that two of them hold.                                |

The workflow's name is its key: 1 to 64 characters of lowercase letters, digits and single hyphens. A workflow needs `instructions` or `skill_sets`; with neither it is refused. One with skill sets and no instructions is a bundle of skills: `get_workflow` reports `has_instructions: false` and there is nothing to run.

**Strict allowlist.** A workflow with a key this server does not know is refused, because a key it does not understand might limit the workflow, and an unknown key never runs. `tools`, `mcp-servers`, `hooks`, `permission-mode`, `model`, `budget` and `schedules` are not part of the schema, so they are refused too: a definition adds words to a session and never widens what the session may do. A key starting `x-` is ignored, for notes and tool metadata.

A client reads workflows two ways, with the same definition and the same `source` shas:

- `get_workflow` with `scopes` alone lists them (name, description, `has_instructions`, and each set's `name`, `path` and `digest`). With `workflow_name` it returns the definition: `instructions`, each set's skill files as `{relative_filepath, file_contents}` and its listed files in `assets`, `warnings`, and `source`. `skill_set` returns that one set, whole, and no `instructions`, so a chat that picks a set never runs the workflow's instructions. `skills: "summary"` lists each inline file's `relative_filepath`, `bytes` and, for a `SKILL.md`, its `name` and `description`, and `skill_path` reads one file by its `<skill>/<file>` path (add `skill_set` to read it inside that set). A reply over the host's 35,000-character tool-result limit comes back as the summary view, which leaves out the listed files and says where to get them, or with a message and no content when even that is too large; REST has no such limit.
- `GET …/packages/{pkg}/workflows` lists, `…/workflows/{name}` reads one (`?skills=full|summary`, `?skillSet=`), and `…/workflows/{name}/skills/{path}` reads one file by its `<skill>/<file>` path (`?skillSet=` looks it up in that set). Pass the `source.versionId` of a read as `?versionId=` on the next, so each read answers from the version the definition did. A repeated or empty `skillSet` is a 400.

### The default workflow

A workflow named `default` holds the package's own skills. Its skill sets' skills, beside the bundled ones, are what `get_skill`, `get_context` and the `…/skills` routes advertise to every session on the package, with no one naming a workflow. A package that declares no `default` has no package skills. Put guidance in `default` only when it applies to every use of the package (what a measure counts, vocabulary, house rules); guidance for one job belongs in that job's workflow. A `default` with `instructions` still serves them through `get_workflow`, and adopting them is still the user's choice.

### Skill sets

A skill set is a folder whose subfolders are skills (`<set>/<skill>/SKILL.md`). Its name is its folder's name, which is what a chat picks it by and what `skill_set` (MCP) or `skillSet` (REST) takes. By convention a set lives at `workflows/<workflow>/skill_sets/<set>/`, but any folder inside the package works, including a root `skills/` folder.

- The folder name meets the same rule as a workflow name, and two entries of one workflow may not share a name or a path.
- Several workflows can list the same set. Publisher reads it once per load, and every workflow that lists it serves the same bytes.
- A skill name that two sets of one workflow both hold is served from the first-listed set when the workflow is read whole, and the workflow's `warnings` say so. Read with `skill_set` (`skillSet` over REST), a set is served whole, every skill it holds included, and its `digest` pins exactly that.
- A set that holds no skills is a warning, not a refusal.
- A `publisher.json` at or above a set's root (the package's own at its root does not count) refuses the workflows that list it, because it would make the folder a second package root. One inside a skill (at the skill's root, in `reference/` or `references/`, or below `assets/`) sets that skill aside with a finding; one elsewhere in a skill only skips that file. One inside a dot-named directory is not read.

Skills are read with the rules in [Skills](#skills). Each set is measured on its own, and the limits are in the [Budgets](agent-skills/authoring-skills.md#budgets) table: 256 KiB of inline Markdown, and 4 MiB and 200 listed files, per set. The 8 MiB cap on listed files for the whole package is charged once per distinct set, in sorted path order, so reordering workflows never moves a set's `digest`.

### A broken workflow never fails the package

Publisher never takes a package offline for a workflow mistake. What breaks decides how much is lost, and each finding is in the package's load warnings with a `Fix:` line.

- **The workflow is refused** (not served; `Workflow '<name>' is not served: <why>`): a bad name, a key Publisher does not know, an entry that is not an object, an empty `description`, an `instructions` file that is missing, not `.md`, over 16 KiB, outside the package or under a nested `publisher.json`, a bad `skill_sets` list (not a list, more than 8, a folder that is missing, outside the package, badly named, listed twice, or holding or sitting under a nested `publisher.json`), a set folder that cannot be read, or nothing to serve. Other workflows and the models keep serving.
- **One skill is skipped** (the workflow still serves, with a finding on its set): a `SKILL.md` that cannot be read or is not UTF-8, a frontmatter `name` that is not its directory name, a `SKILL.md` that would take the set's inline Markdown past 256 KiB, a `publisher.json` inside the skill, or a strict finding under `assets/` (a link, a link out of the skill, `..` or two names that differ only by case, a `SKILL.md`). A file that `.skillignore` or a walk budget hides is never looked at, so a `SKILL.md` below `assets/` that either one hides sets nothing aside; a `publisher.json` is the exception, found whether or not `.skillignore` matches it.
- **One file is skipped** (the skill still serves): a file that breaks a limit, a reserved name, or a path or walk past its bound. See [Which files a skill holds](#which-files-a-skill-holds).
- **A name repeated across a workflow's sets** is served from the first-listed set, with a finding on the workflow.

A skill with no `description` is served but flagged, since nothing tells a caller when to read it.

### Migrating from agents and a root `skills/`

Package agents are gone: the `agents` key, its schedules, budget and model, tasks, `get_agent` and the `…/agents` routes. A manifest that declares `agents` loads with the warning `"agents" is no longer read; declare "workflows"`. A root `skills/` directory is no longer read on its own either. A package with one that no workflow lists as a skill set loads with the warning `skills/ is no longer read; move each skill into a skill set and list it under "workflows.default"`. Both warnings carry a code (`PACKAGE_AGENTS_NOT_READ`, `PACKAGE_SKILLS_DIR_NOT_READ`), and each load that hits one is counted in `publisher_deprecated_usage_total` (features `agents` and `skills_dir`).

To migrate a package:

1. Each old agent task becomes its own workflow, with the task file as `instructions`. A task's name is the workflow's name (`weekly-report.md` becomes `workflows.weekly-report`); move the file to `workflows/<name>/instructions.md`.
2. An old agent's standing guidance (its `instructions`) moves into a skill, in a set of its own (`workflows/<name>/skill_sets/<set>/<skill>/SKILL.md`), with a one-sentence `description` saying when to read it. The agent's own `skills/<x>/` folders move into that set too. List the set in each workflow that needs it.
3. Root `skills/<x>/` move to `workflows/default/skill_sets/<set>/<x>/`, and `default` is declared with that set. Declaring `"default": { "skill_sets": ["skills"] }` instead keeps the folder where it is.
4. Drop `schedules`, `budget` and `model`. Nothing runs a workflow on a schedule, and nothing sets a model or a budget. A recurring job needs a scheduler of your own that fetches the workflow.
5. Delete `agents/` and the old root `skills/` in the same change, unless `default` lists `skills`.

### What a digest covers

Two hashes say which bytes a definition came from. `sourceContentSha`, in the package's serving state (`GET …/packages/{pkg}/status`), covers the models, the `workflows` object, every file a served workflow or skill set reads, and each applied `.skillignore`; a `version` bump or another manifest key does not move it. On a workflow, `definitionSha` covers its name, description, the path and text of its instructions, and each skill set's path and `digest`. A set's `digest` covers its path and what it serves: each file's digest and each skill's applied `.skillignore` text. So editing one workflow, one set or a model leaves alone the `definitionSha` of every workflow that does not list what changed. The digests hash a file's bytes, not what is served from it, so a change to a `#` comment or a YAML escape in a `SKILL.md`'s frontmatter moves them even though the served `name` and `description` are the same. `GET …/workflows/{name}` returns both hashes under `source`, with `servedRevision`.

## Skills

A skill is a directory with a `SKILL.md` in the [agentskills.io](https://agentskills.io) format (a `name` and `description` in the frontmatter, then the body). A skill is its whole directory: the spec's `scripts/`, `references/` and `assets/` folders, and any other file under it, are part of the skill, and the body names them by paths relative to the skill root. A package's skills live in the skill sets of its workflows, and the `default` workflow's are served next to the bundled skills, scoped to that package, so an agent learns how to work with this package's data from the package itself:

- `get_skill(skill_name, path)` reads any file of a skill by its path, for example `references/joins.md` or `scripts/check.py`. `path` defaults to `SKILL.md`. Reading `SKILL.md` also returns the parsed `frontmatter` and `files`, every file of the skill as `{path, size}`, which is the skill's served set unless the reply says some entries were cut to keep it under the 35,000-character tool-result limit. A binary file answers with its size and a message, never its bytes.
- `GET …/packages/{pkg}/skills` lists the skills, `…/skills/{skill}` reads `SKILL.md` and `…/skills/{skill}/{path}` reads one file. `content` is the file as written, frontmatter included, with nothing appended. A listed file (not Markdown, or Markdown that was listed) that changed on disk since the package loaded answers 404 until the package reloads; inline Markdown is served from the text read at load.
- A workflow's own reads (`get_workflow`, `…/workflows/{name}`) carry the same skills per set. There a `SKILL.md` is re-emitted with only a quoted `name` and `description` in its frontmatter, and the other Markdown is served as written.

Rules for what is served:

- A skill whose `name` matches a bundled skill replaces it for this package. Nothing is merged. Every entry carries an `origin` (`package` or `bundled`) saying which one you are reading.
- Every file is read through its real path, and one that resolves outside the package is not served, so a link cannot pull in a file from elsewhere on the machine. A link to a Markdown file inside the package is followed; any other link is skipped with a warning. A skill set folder that is itself a link out of the package refuses the workflows that list it.
- A skill whose `SKILL.md` cannot be read or is not UTF-8 text, or whose frontmatter `name` differs from its directory name, is skipped with a finding on its set. One with no `description` is served but flagged. A file that breaks a limit is skipped alone, without dropping its skill. Each case is a load warning naming the fix, and the package still loads.
- The frontmatter is read as YAML 1.2. One that does not parse, repeats a key, is over 4 KiB, or expands past 5 times its source plus 10,000 characters is served as an empty `frontmatter`, with `name` and `description` read line by line and a warning.

### Which files a skill holds

A skill is every file under its directory except these, which never reach an agent:

- Dotfiles and dot-folders (`.skillignore` itself, `.env`, `.DS_Store`, `.git/`, `.claude/`), `node_modules/`, `__pycache__/` and `*.pyc`. No `.skillignore` pattern brings one back. Rename the file (`env.example`) to ship it.
- Whatever `.skillignore` at the skill's root matches. It takes gitignore syntax, matched without regard to case, and only the root file is read. `SKILL.md` stays whatever it says, with a warning.
- A `SKILL.md` below the root, and `CLAUDE.md`, `CLAUDE.local.md`, `AGENTS.md` and `publisher.json` anywhere in the skill, in any case. An agent computer reads those files with authority, so each is skipped with a warning. A `publisher.json` at the skill's root, in `reference/` or `references/`, or below `assets/`, and a `SKILL.md` below `assets/`, set the whole skill aside instead.
- A `.malloy` or `.malloynb` file, which is a model and not skill content.
- A file that breaks a limit (see [the budgets](agent-skills/authoring-skills.md#budgets)), each named in a warning.

`.skillignore` and `.credignore` decide different things:

| | `.skillignore` | `.credignore` |
| --- | --- | --- |
| Decides | What an agent is given: the files a skill lists, hashes, serves and installs | What `cred publish` uploads |
| Sits | At the skill's root, `<set>/<skill>/.skillignore` | At the package root, beside `publisher.json` |
| Dialect | Full gitignore, including `!` to bring a file back | A path or glob per line; refuses `!` |
| Case | Matches without regard to case | Matches without regard to case |

Editing `.skillignore` moves the set's `digest`, the `definitionSha` of every workflow that lists it and the package's `sourceContentSha`, as editing any skill file does. The file is bounded: at most 16 KiB, 50 patterns of at most 200 characters each and 1,024 together (comments and blank lines are free), and two `*` or `**` segments in a pattern (a run of `**` counts once). Every pattern runs against every path the walk examines, so these, with the walk's entry limits and a limit of 200 characters on a path from the skill's root (an entry past it is skipped with a warning, and a directory past it is not descended), bound the time a skill's walk can take. Ignored entries still count toward the 1,000-entry walk limit, and a Markdown file in `reference/` or `references/` counts toward its 1,000 whether or not `.skillignore` matches it. A pattern past a bound is skipped with a warning, and one that starts with `./` warns because it matches nothing.

**An ignored file is still in the package.** `.skillignore` curates what an agent is given; it is not a way to hide a secret. The file stays in the package copy that Publisher holds and that an agent computer can read, and `.credignore` alone decides what is uploaded. Keep a secret out with `.credignore`, or do not commit it. A skill that another client reads from disk (Claude Code from `.claude/skills`, `npx skills`) is read whole, because those clients do not read `.skillignore`.

To see what a skill holds before an agent does:

- A package skill: `GET …/packages/{pkg}/skills/{skill}`, whose `files` is the served set, or `get_skill` on its `SKILL.md`, which trims `files` past 35,000 characters and says so.
- A workflow's skills: `get_workflow` with `skills: "summary"` lists each inline file and, in the full view, each listed file with its size and sha256.
- The upload: `cred publish --dry-run` lists the files that would be uploaded. That is the upload set, not the skill set.

### Listed files

A skill carries its Markdown inline: `SKILL.md` and every `*.md` file outside `assets/`, at any depth, while they fit the set's 256 KiB inline budget. Every other file of the skill is listed: scripts, templates, data, images, and any Markdown under `assets/`, that does not fit the budget, or that is not UTF-8. Publisher lists each one without its contents, in the set's `assets` (`{relative_filepath, bytes, sha256}`, with the path relative to the set's folder; the summary view leaves `assets` out). It serves a text file on request through the skill-file route (`GET …/workflows/{name}/skills/<skill>/<path>`, MCP `skill_path`, or `get_skill` with `path`). A binary file answers with its size and a `message`, never its bytes, so a caller copies it from the package. Listed files never count against the 256 KiB Markdown budget.

Every limit is in one table, [Budgets](agent-skills/authoring-skills.md#budgets). Problems fall in two classes. **Security** problems set the whole skill aside, with a finding on its set naming the file and the fix: a link anywhere under `assets/`, an `assets/` that is itself a link or resolves outside the skill, a nested `publisher.json` or `SKILL.md` under `assets/`, `..` in a name under `assets/`, or two paths under `assets/` that differ only by case. **Everything else** skips that one file, leaves it out of the listing and warns naming the file and the limit: a link elsewhere in the skill, 1 MiB or more, over a file or byte budget, a bad segment name or a path over 200 characters, a Malloy extension, a reserved name, or a walk past the depth or entry limit. A set with a very large skill directory is still served. The caps bound what is listed, not every byte examined: a file under the size cap is read to check it, and each file is read and hashed at most once per load however many workflows share its set; a skipped file is never charged to a budget.

**Trust.** A listed file is copied unread. A script runs as a command in the agent's session, and an asset copied into an app runs with the viewing user's data authority wherever that app opens, previews while it is being built included. Review listed files as code before adopting a workflow that ships them.

## Where the data comes from

Every loaded package automatically gets a DuckDB connection named `duckdb`, rooted at the package
directory. `duckdb.table('data/sales.csv')` works with zero configuration, and so do
`.parquet`, `.json`, `.ndjson`, and `.xlsx` files read the same way. Relative paths resolve
against the package root. It is also the default connection, so a model that names no connection
gets it.

There is no preprocessing step for any of these: DuckDB reads them in place, so a file never needs
converting to CSV, and inspecting one with a script instead of querying it is always the slower
path. JSON carries no schema, so a value written as `"90"` arrives as a string where CSV would
infer a number; cast it in the source with `::number`. Anything needing read options (an Excel
sheet name, a JSON format hint) goes through `duckdb.sql("""SELECT * FROM read_json_auto(...)""")`
instead of `table()`.

A package cannot declare its own warehouse connection. Connections to BigQuery, Snowflake,
Postgres, and the rest are defined per environment, in the server's config; the name `duckdb` is
reserved for the per-package sandbox. See [connections.md](connections.md).

## Serving a package

There is no directory scan: a package on disk serves only once it is registered. Two main ways:

- A `{ "name": "...", "location": "..." }` entry under an environment in `publisher.config.json`.
  The `location` can be a local path (absolute, `~/`, or relative to the config file, written
  `./sales`) or a `https://github.com/...`, `gs://`, or `s3://` URL. See
  [configuration.md](configuration.md#bring-your-own-config) for the full recipe.
- On a running server, `POST /api/v0/environments/{env}/packages` with
  `{"name": "...", "location": "/absolute/path"}`. See [api-overview.md](api-overview.md). Like
  the rest of the API this endpoint is unauthenticated, so keep the server on localhost or behind
  a gateway.

### Annotations that do nothing

Malloy decides which reader sees an annotation by its prefix, so a small slip makes one silently do
nothing: `#(clasification) "phi"`, `# (doc) …` with a space before the bracket, `#@persist` with no
space, `# persist` or `#(persist)` for `#@ persist`, or `#@ persits`. Each loaded model reports these
with the line and the spelling it meant:

- `annotation-near-miss` (a `warn` in the package's `warnings`): an annotation that reads as an
  attempt at a route Publisher reads (`#(doc)`, `#(markdown)`, `#(classification)`, `#(carries)`,
  `#(secure)`, `#(index)`, `#(dbmeta)`) but does not reach it.
- `materialization-near-miss` (a load failure): a near miss of a materialization route or directive,
  `#@ persist`, `#@ preaggregate`, and, with `PUBLISHER_MATERIALIZE` on, `#(materialize)` and
  `#(rollup)`, so `#(materialise)`, `#(Materialize)` and `#(preaggregates)` are caught. A
  declaration that silently builds nothing is a mistake in the model, so it fails the publish and
  the load ([materialization.md](materialization.md#fails-the-load-or-refused-alone)). With
  `PUBLISHER_MATERIALIZE` off, `#(materialize)` and `#(rollup)` spelled exactly are not read,
  which is the deployment's choice rather than a mistake: `#(rollup)` is then an
  `annotation-near-miss` warning reaching for `#@ preaggregate`.
- `annotation-malformed-route`: a prefix Malloy cannot route at all, such as `#percent` (write
  `# percent` for a tag, or `#(name)` for an app route). Malloy warns about these when it compiles,
  and the package now carries the warning.

Near misses of other routes are warnings, not refusals: a route that only looks like one of these
may belong to another application, and a publish still succeeds. Materialization routes and gates
are the exceptions, because a declaration loaded inert builds nothing, and a gate loaded inert
leaves the source open. The spellings Publisher already refused (`#(AUTHORIZE)`, `# (authorize)`,
`#(access-filter)` and the like, see [authorize.md](authorize.md)) still fail the load, and so now
does a misspelled gate: `#(authorise)`, `#(autorize)`, `#(acess_filter)`, any route word within two
edits of `authorize` or `access_filter`, on a source, a field or a named query. A word that contains
a gate's name, such as `#(authorized)` or `#(reauthorize)`, reads as another application's route and
only warns. Each model reports only the annotations in its own file, so a file several models import
is reported once. `compile_model` at `file` scope returns the near misses as `warn` diagnostics at
their line, and a misspelled gate or materialization route as an `error` (code
`annotation-gate-near-miss` or `materialization-near-miss`), so an editor sees the refusal before a
save. A capitalized directive (`#@ Persist`, `# Persist`) is a materialization
near miss too: Malloy reads only the lower-case `persist`. They are
counted in `publisher_annotation_findings_total` with the other annotation findings.

## Lifecycle: what is served

When an environment is first created, Publisher publishes each configured package as the version
its `publisher.json` declares, into its own storage at `publisher_data/<env>/<pkg>/<version>/` (in
the server root: the directory the server was launched from, unless `--server_root` set another),
compiles it, and serves it. A package that declares no `version` fails to load. Consequences:

- A published version is immutable. Editing your original source directory afterwards changes
  nothing, and neither does a reload: `GET …/packages/{pkg}?reload=true` or `reload_package`
  answers the version unchanged. To ship an edit, bump `version` in `publisher.json` and publish
  again. See [package-versions.md](package-versions.md).
- In watch mode (`--watch-env <env>`), the environment's local packages are mounted in place as
  symlinks instead, with no versions, and an edit is read: reload the package to recompile it,
  `GET /api/v0/environments/{env}/packages/{pkg}?reload=true` over REST or the `reload_package`
  MCP tool. A reload that fails to compile leaves the files alone and keeps serving the previous
  model.
- An unversioned (for example watch-mode) package's reload, and a publish, read the columns of
  each table the package reads from the warehouse again, so a column added, dropped or retyped
  there shows up. The recompile watch mode runs on a save, and a boot, reuse the columns already
  fetched: reload the package to see a warehouse change. With versioning on (the default) a
  version's reload changes nothing, so a column change is picked up by publishing a new version.
- `--init` deletes `publisher_data/` and publishes everything again from the configured
  locations. Keep your source of truth outside `publisher_data/`.

### Installed packages (versioning off)

With `PUBLISHER_PACKAGE_VERSIONING=off`, a package can also arrive through the API: a `POST` to an environment's `packages` with a
`location`, or a `PATCH` on a package that names one. The server downloads it, compiles the new
copy beside the one that is serving (if any), and swaps it in; the previous copy answers queries
until the swap. Four things follow from how that is recorded:

- The server writes where it fetched the package from into its own file outside the package
  directory, `publisher_data/<env>/.install-records/<pkg>.json`. It never writes that into
  `publisher.json`, and nothing inside the package is read as one, neither a `location` an author
  puts in `publisher.json` nor a record file shipped with the content: a reload re-fetches from
  the recorded location, so only the server may set it.
- A `PATCH` whose `location` equals the recorded one is a metadata update. Nothing is fetched or
  recompiled beyond what a new `manifestLocation` requires. A `PATCH` with a different `location`
  installs from it. A `PATCH` never changes the recorded location by itself. To fetch the same
  location again, reload the package.
- A `PATCH` naming a `manifestLocation` that cannot be fetched or bound is refused, with 400
  `MANIFEST_NOT_FOUND` when nothing is at it, 422 `MANIFEST_INVALID` when what is there is not a
  build manifest, and otherwise 500 `MANIFEST_BIND_FAILED`; nothing of that `PATCH` is applied: `publisher.json` and the
  package's metadata stay as they were, and it serves what it served before. A manifest that binds
  with a table declined (one the warehouse does not hold) answers 200, with that source live.
- A `PATCH` that arrives while a load of the package is in progress waits for it, and is applied
  to whichever copy that load leaves resident. So it lands on the copy being installed, or, if the
  install failed, on the copy that is still serving.
- A reload of an installed package re-fetches from the recorded location and re-applies the
  manifest binding it had. Everything else, the description, `explores`, policy, is what the
  re-fetched `publisher.json` declares.

While a package is loading, reinstalling or recompiling, the `status` on its entry in
`GET /api/v0/status` reports `loading: true`
alongside `serving`, which stays true for as long as a previous copy answers queries. A package
loading for the first time is listed only by `GET /api/v0/status?includeLoading=true`; every other
listing shows packages that can serve. See [api-overview.md](api-overview.md).

The agent workflow built on this lifecycle is in [AGENTS.md](../AGENTS.md); load failures and how
to read them are in [deployment.md](deployment.md#serving-does-not-mean-everything-loaded).

## See also

- A `public/` directory of HTML pages makes the package a data app, no build step:
  [html-data-apps.md](html-data-apps.md).
- Runtime parameters and access control, declared in the models:
  [givens.md](givens.md), [authorize.md](authorize.md), [row-level-access.md](row-level-access.md).
