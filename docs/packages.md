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
| `skillSets`        | Named skill sets the package declares, keyed by set name: `{ path, description?, overrides? }`. Its `default` set holds the package's own skills. See [Skill sets and workflows](#skill-sets-and-workflows). |
| `workflows`        | Named workflows the package declares, keyed by workflow name: `{ description?, instructions, skillSets? }`, where `skillSets` lists set names. See [Skill sets and workflows](#skill-sets-and-workflows). |
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

## Skill sets and workflows

A package can declare **skill sets** (folders of skills, each read once and served by name) and **workflows** (named jobs its author wrote: a Markdown brief plus the skill sets to use). Publisher parses and serves both. It does not run a workflow. A client adopts one by fetching it (`get_workflow`, or `GET …/packages/{pkg}/workflows/{name}`) and following it in its own session, with its own tools and permissions; the bundled `malloy-run-workflow` skill is the recipe, and `malloy-define-workflow` covers writing a definition. Adopt a workflow only when the user names it: nothing advertises one in discovery. The skill set named `default` is the exception: its skills are the package's own skills ([below](#the-default-skill-set)).

The shortest package declares one set, `default`, at a folder of skills:

```
publisher.json
skills/
  margin-rules/
    SKILL.md
```

```json
{
  "name": "storefront",
  "skillSets": { "default": { "path": "skills" } }
}
```

`skills/margin-rules/SKILL.md` is a complete skill. `name` equals the directory's name, `description` is one sentence saying when to read it, and the body is what the agent reads:

```markdown
---
name: margin-rules
description: Use when a question involves margin, so net revenue is not mistaken for gross.
---

Margin is net revenue less cost, never gross revenue less cost. `margin_pct` in `orders.malloy` already does this; use it.
```

A package adds a named set and a workflow that lists it. A set's name is its key in `skillSets`, whatever its folder is called:

```json
{
  "name": "storefront",
  "skillSets": {
    "default": { "path": "skill_sets/default", "description": "House conventions" },
    "analyst": { "path": "skill_sets/analyst" }
  },
  "workflows": {
    "weekly-report": {
      "description": "Writes the weekly storefront report as the analyst",
      "instructions": "workflows/weekly-report/instructions.md",
      "skillSets": ["analyst", "default"]
    }
  }
}
```

The [storefront example](../examples/storefront) is laid out this way, with its `default` set overriding one bundled skill.

| Key under `skillSets.<name>` | Meaning                                                                                                                                                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `path`                       | Required. A package-relative folder whose subfolders are skills (`<path>/<skill>/SKILL.md`). `skills` reads the root `skills/` folder.                                                                                                |
| `description`                | Optional. A non-empty line saying what the set holds.                                                                                                                                                                                |
| `overrides`                  | Optional. Up to 64 distinct skill names, read only on `default`, where it lets a skill of that name replace the bundled skill of the same name ([below](#the-default-skill-set)). A bad value reads as empty and adds one `skill-finding`; on any other set a non-empty `overrides` is ignored, with one `skill-finding`.        |

| Key under `workflows.<name>` | Meaning                                                                                                                                                  |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `description`                | Optional. A non-empty line saying what the workflow is for.                                                                                              |
| `instructions`               | Required. Package-relative path of a `.md` file: the brief the workflow works from. At most 16 KiB.                                                      |
| `skillSets`                  | Optional. At most 8 distinct names of declared sets, in the order an installer reads them.                                                               |

A set's and a workflow's name is its key: 1 to 64 characters of lowercase letters, digits and single hyphens. A workflow named `default` is an ordinary workflow. A set that two entries declare at one folder is refused, both of them, and a set declared twice under one name keeps its last entry, because JSON collapses duplicate keys.

**Strict allowlist.** A set or workflow with a key this server does not know is refused, because a key it does not understand might limit it, and an unknown key never runs. `tools`, `mcp-servers`, `hooks`, `permission-mode`, `model`, `budget` and `schedules` are not part of either schema, so they are refused too: a declaration never names a tool or a permission. A skill file in a set can still carry such keys in its frontmatter, which is what the install rule below is for. A key starting `x-` is ignored, for notes and tool metadata.

**Bundled names.** The `malloy-` and `eval-` prefixes belong to the skills this server bundles, and a future bundled skill may take any name under them, so pick names outside both. A package skill that takes a bundled name needs `overrides` on `default`, or a rename.

A client reads both kinds two ways, with the same bytes and the same `source` shas:

- `get_skill_set` with `scopes` alone lists the sets (`name`, `description` and `digest`). With `skill_set` it returns that set whole: each skill file as `{relative_filepath, file_contents}` (a `SKILL.md` entry also carries its `name` and `description`), its listed files in `assets`, its own `warnings` and `source`. `skills: "summary"` lists each inline file's `relative_filepath`, `bytes` and, for a `SKILL.md`, its `name` and `description`, and `skill_path` reads one file by its `<skill>/<file>` path.
- `get_workflow` with `scopes` alone lists the workflows (`name`, `description`, `skill_set_names`). With `workflow` it returns the definition: `instructions`, every listed set exactly as `get_skill_set` returns it, the workflow's `warnings` and `source`. A reply over the host's 35,000-character tool-result limit comes back as the summary view, which leaves out file text and the listed files and says where to get them; when even that is too large, as the instructions and each set's `name` and `digest`; and when that is too large, as the workflow's `name` and `source` with a message and no content. `get_skill_set` steps down the same way without instructions. REST has no such limit.
- `GET …/packages/{pkg}/skill-sets` lists, `…/skill-sets/{name}` reads one (`?skills=full|summary`), and `…/skill-sets/{name}/skills/{path}` reads one file by its `<skill>/<file>` path (`<skill>/` reads that skill's `SKILL.md`). `GET …/packages/{pkg}/workflows` lists and `…/workflows/{name}` reads one (`?skills=full|summary`). Pass the `source.versionId` of a read as `?versionId=` on the next, so each read answers from the version the definition did.

**Installing a skill set.** A `SKILL.md` arrives exactly as written, frontmatter included, and no route strips a key from it. An installer writes only `name` and `description` into the installed file's frontmatter, taken from the entry's fields (the description JSON-quoted, left out when the entry has none), then the text after the file's own frontmatter (the `---` block that opens line 1, after one byte-order mark, with CRLF read as LF), and names any other frontmatter key the file carried (`allowed-tools`, `hooks`) to the user. The bundled `malloy-run-workflow` skill does exactly this and never overwrites a skill the user already has. See [Security posture](security-posture.md) for why the rule matters.

### The default skill set

The set named `default` holds the package's own skills. Its skills, beside the bundled ones, are what `get_skill`, `get_context` and the `…/skills` routes advertise to every session on the package, with no one naming a workflow. A package that declares no `default` set has no package skills. Put guidance in `default` only when it applies to every use of the package (what a measure counts, vocabulary, house rules); guidance for one job belongs in a workflow's `instructions` and the set it lists.

**`overrides` decides one thing.** A `default` skill named like a bundled skill is served in the bundled skill's place only when `default`'s `overrides` lists the name; otherwise the bundled skill is served and the package's copy stays out of `get_skill`, `…/skills` and `get_context`. That is the only place `overrides` changes what Publisher serves. A set read (`get_skill_set`, `…/skill-sets/{name}`) and a workflow read serve the set whole, the overriding skill included, whatever `overrides` says. An unlisted bundled name gets a `skill-finding` on its set naming the fix. Only `default` can replace a bundled skill, so in any other set a skill named like one gets a finding that says to rename it, and its `overrides` is ignored. Every entry carries an `origin` (`package` or `bundled`) saying which one you are reading. Credible's agent keeps its platform copy of a bundled skill and skips a package skill of that name, so a package that must behave the same in both gives its skill a name no bundled skill has.

### Skill sets

A set is a folder whose subfolders are skills (`<set path>/<skill>/SKILL.md`). Any folder inside the package works, so `"analyst": { "path": "analyst" }` with `analyst/<skill>/SKILL.md` is the shortest layout, and `"path": "skills"` reads a root `skills/` folder. By convention a package with several sets keeps them at `skill_sets/<set>/`.

- Every declared set is read once per load, watched, and served at its own route, whether or not a workflow lists it. A set is served whole or refused whole, and its served content depends only on its own files and declaration.
- Several workflows can list the same set; each serves the same bytes.
- A skill name that two sets of one workflow both hold is in both sets, each served whole, and the workflow's `warnings` carry a `skill-shadowed` finding. An installer keeps the first-listed set's copy.
- A folder that holds a `SKILL.md` itself is a skill, not a set. Declaring it loads nothing and gets the finding "this is a skill, not a skill set"; declare its parent folder instead (a skill at the package root's top level has no parent to declare: move it into a folder and declare that folder), or, if its subfolders are the skills, remove its `SKILL.md`.
- A `publisher.json` at or above a set's root (the package's own at its root does not count) refuses the set, because it would make the folder a second package root. One inside a skill (at the skill's root, in `reference/` or `references/`, or below `assets/`) sets that skill aside with a finding; one elsewhere in a skill only skips that file. One inside a dot-named directory is not read.

Skills are read with the rules in [Skills](#skills). Each set is measured on its own, and the limits are in the [Budgets](agent-skills/authoring-skills.md#budgets) table: 256 KiB of inline Markdown, at most 1,000 inline files, 64 skills, and 4 MiB and 200 listed files, per set. A package declares at most 32 sets, and a workflow lists at most 8. The 8 MiB cap on listed files for the whole package is charged set by set, in name order; a set that would cross it is refused whole, and so is every workflow that lists it.

### A broken set or workflow never fails the package

Publisher never takes a package offline for a skill set or workflow mistake. What breaks decides how much is lost. Each finding is in the package's load warnings, with a `code` and a `subject`, and a set's own findings also travel on that set (`SkillSet.warnings`) and a workflow's shadowing findings on the workflow. Most end with a `Fix:` line saying what to change. These do not: a skipped file (it names the file and says why), a skill set aside by an `assets/` rule or by a `publisher.json` inside it (the reason says what to remove), a problem with a skill's own `.skillignore` or a walk bound it reached, or a directory it could not list (each says what was left out), a Markdown file listed instead of inlined, and the lines counting the findings or skipped files a set leaves unnamed.

| `code`                   | `subject`        | What it says                                                                                                                                                                                                                                                   |
| ------------------------ | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `workflow-not-served`    | the workflow's name | The workflow's own declaration is refused, or it lists an undeclared or refused set; `message` is the refusal.                                                                                                                                              |
| `skill-set-not-served`   | the set's name   | The set's declaration is refused (a key, a path, a description, a duplicate path), or it would cross the 8 MiB package cap; `message` is the refusal.                                                                                                          |
| `skill-shadowed`         | the workflow's name | Two of its sets hold a skill of one name. Both are served whole; an installer keeps the first-listed set's copy. Also on `Workflow.warnings`.                                                                                                              |
| `skill-file-skipped`     | the set's name   | A served skill has a file of its directory left out: a reserved name, a link, a file over a limit, a file over the reply budget of `get_skill`. Also on `SkillSet.warnings`.                                                                                    |
| `skill-finding`          | the set's name   | Any other finding of a set: a skill that is not served, a skill served with a gap (no `description`, frontmatter read line by line), a folder that is a skill and not a set, a set with no skills or past its 64-skill cap, a bundled name not in `overrides`, a bad `overrides` value, an `overrides` on a set that is not `default`, a Markdown file listed instead of inlined. Also on `SkillSet.warnings`. |
| `skill-sets-unavailable` | none             | The walk could not finish (below): no set or workflow is served until the package reloads.                                                                                                                                                                     |

A set's findings are published once however many workflows list it. Each refused workflow and each refused set is also logged once per load at `warn`, with the package name, the workflow or set name and the code.

- **A workflow is refused** (not served; `Workflow '<name>' is not served: <why>`): a bad name, a key Publisher does not know, an entry that is not an object, an empty `description`, an `instructions` file that is missing or not `.md`, over 16 KiB, outside the package, a link loop or a name too long, or under a nested `publisher.json`, a bad `skillSets` list (not a list, more than 8, a name listed twice, a name no set declares, or a set that is refused). Other workflows, the sets and the models keep serving.
- **A set is refused** (not served; `Skill set '<name>' is not served: <why>`): a bad name, a key Publisher does not know, a `path` that is missing, a link loop or a name too long, a file, outside the package, or holding or sitting under a nested `publisher.json`, an empty `description`, a path another set also declares, or a set that would take the package past 8 MiB of listed files. Every workflow that lists it is refused too.
- **The whole declaration is refused**: `skillSets` or `workflows` is present and not an object, there are more than 32 sets or 64 workflows, or the two together are over 1 MiB or too deeply nested. Then every set name and every workflow name answers `*_NOT_SERVED`, with the message of the package warning that says why (the one warning with no `code`).
- **The walk cannot finish** (nothing is refused, and nothing is served until the package reloads): a skill set folder or an `instructions` file that exists but cannot be read (`EACCES`, `EIO`), a package path that no longer resolves, or a walk that throws. The models still load, one `skill-sets-unavailable` warning names the cause, every workflow route and every set route answers 409 `SKILL_SETS_UNAVAILABLE` with the reason, and so do `get_workflow` and `get_skill_set`. The skills routes, scoped `get_skill` and `get_context`'s skill note answer 409 too when the package declares a `default` set, even for a bundled skill's name, because a `default` skill can replace a bundled one and the walk that would say so did not finish (without a `default` set the bundled skills serve as usual). A name the manifest never declares, or declares and Publisher refuses for what the author wrote, is still a 404, with its `reason`. Fixing the cause and reloading serves everything again.
- **One skill is skipped** (the set still serves, with a finding on it): a `SKILL.md` that cannot be read or is not UTF-8, a frontmatter `name` that is not its directory name, a `SKILL.md` that would take the set's inline Markdown past 256 KiB, a `publisher.json` inside the skill, a skill directory whose name has a control character, `..` or a character outside `[A-Za-z0-9_][A-Za-z0-9._-]*`, a directory that differs from another skill's only by case (the second is skipped), a skill whose `assets/` the search for a `publisher.json` cannot finish within the set's 10,000-entry budget (it counts every entry it lists, and sets aside the skill that runs it out and each later skill of the set with anything under `assets/`), or a strict finding under `assets/` (a link, a link out of the skill, `..` or two names that differ only by case, a `SKILL.md`). A file that `.skillignore` or a walk budget hides is never looked at, so a `SKILL.md` below `assets/` that either one hides sets nothing aside; a `publisher.json` is the exception, found whether or not `.skillignore` matches it.

A 404 from a set or workflow route says why. `GET …/skill-sets/{name}` and `GET …/skill-sets/{name}/skills/{path}` carry a `reason`, and so does `get_skill_set`'s reply (with `skillSet: null`), when the set is not found: `SKILL_SET_NOT_SERVED` when the manifest declares it and Publisher refuses it (the message is the refusal, the text of its `skill-set-not-served` warning), and `SKILL_SET_NOT_DECLARED` when the manifest declares no set of that name. The workflow routes and `get_workflow` do the same with `WORKFLOW_NOT_SERVED` and `WORKFLOW_NOT_DECLARED`. A served set with no file of the path asked for is a 404 with no `reason`.

A skill with no `description` is served but flagged, since nothing tells a caller when to read it.

### Migrating from agents and a root `skills/`

Package agents are gone: the `agents` key, its schedules, budget and model, tasks, `get_agent` and the `…/agents` routes. A manifest that declares `agents` loads with a warning that points at this section. A root `skills/` directory is no longer read on its own either. A package with one that no declared skill set is at or under loads with a warning that names the one-line fix below. A published version is immutable, so a version with a root `skills/` serves no package skills on this release until it is republished with that declaration. Both warnings carry a code (`package-agents-not-read`, `package-skills-dir-not-read`), and each load that hits one is counted in `publisher_deprecated_usage_total` (features `agents` and `skills_dir`). The count is of loads, not packages. Before you upgrade, find the affected packages by scanning stored packages for an `agents` key in `publisher.json` or a root `skills/` folder; after it, the two codes on a package's `warnings` name each one.

The quickest migration of a root `skills/` moves no file: add `"skillSets": { "default": { "path": "skills" } }` to `publisher.json`. The folder must hold `<skill>/SKILL.md`. Two things differ from the old root read. A skill named like a bundled skill replaces it only if `default`'s `overrides` lists the name, so list each one that should. And every file of a skill is served now, so put what a skill must not serve in a [`.skillignore`](#which-files-a-skill-holds) at its root. The steps below also move files into the `skill_sets/<set>/` layout.

**Always-on guidance becomes on-demand.** An agent's `instructions` reached every session on the package. A skill is read when its `description` matches the job, and a workflow's `instructions` only when a user names the workflow, so guidance moved into a skill is no longer always on. Guidance that must still reach every session goes in a skill of the `default` set, which every session on the package sees listed, with a `description` that says it always applies; guidance for one named job goes in that workflow's `instructions`.

To migrate a package:

1. Each old agent task becomes its own workflow, with the task file as `instructions`. A task's name is the workflow's name (`weekly-report.md` becomes `workflows.weekly-report`); move the file to `workflows/<name>/instructions.md`. An agent that had no tasks has no workflow to declare; see the walk-through below.
2. An old agent's standing guidance (its `instructions`) moves into a skill, in a set of its own (`skill_sets/<set>/<skill>/SKILL.md`, declared under `skillSets`), with a one-sentence `description` saying when to read it. The agent's own `skills/<x>/` folders move into that set too. List the set in each workflow that needs it.
3. Root `skills/<x>/` move to `skill_sets/default/<x>/`, and `default` is declared at that folder. Declaring `"default": { "path": "skills" }` instead keeps the folder where it is.
4. Drop `schedules`, `budget` and `model`. Nothing runs a workflow on a schedule, and nothing sets a model or a budget. A recurring job needs a scheduler of your own that fetches the workflow.
5. Delete `agents/` and the old root `skills/` in the same change, unless `default` declares `skills`.

An agent that had no tasks, only standing `instructions` and perhaps `skills/<x>/` folders of its own, migrates to the `default` set:

1. Its `instructions` file becomes `skills/analyst-guide/SKILL.md`: frontmatter with `name: analyst-guide` and a one-sentence `description` saying when to read it, then the old text as the body.
2. Its own `skills/<x>/` folders move beside it, under `skills/`.
3. Declare `"skillSets": { "default": { "path": "skills" } }`, and delete the `agents` key and `agents/`.

What the agent used to get on every session it now gets when `analyst-guide`'s `description` matches the job.

### What a digest covers

Three hashes pin what a client read. `sourceContentSha`, in the package's serving state (`GET …/packages/{pkg}/status`), covers the models, the `skillSets` and `workflows` declaration, every file a served set or workflow reads, as written, and each applied `.skillignore`; a `version` bump or another manifest key does not move it. A set's `digest` covers each served file's path and as-written SHA-256 (inline and listed alike), each skill's applied `.skillignore` SHA-256 and the set's sorted `overrides`; it does not cover the set's name, path or description, so two sets with the same content have the same digest, which makes it safe as a cache key. A set read returns exactly what its `digest` covers. A workflow's `definitionSha` covers its name, its description, the SHA-256 of its instructions and each listed set's name and `digest`, in order. Editing one set leaves every other set's `digest` alone, and a workflow's `definitionSha` moves only when the workflow or a set it lists changes. A change the served text does not carry cannot exist now that a `SKILL.md` is served as written: every byte of it moves the `digest`. `GET …/skill-sets/{name}` returns the `source` shas with `servedRevision`, and `GET …/workflows/{name}` returns them with `definitionSha` as well.

## Skills

A skill is a directory with a `SKILL.md` in the [agentskills.io](https://agentskills.io) format (a `name` and `description` in the frontmatter, then the body). A skill is its whole directory: the spec's `scripts/`, `references/` and `assets/` folders, and any other file under it, are part of the skill, and the body names them by paths relative to the skill root. A package's skills live in its skill sets, and the `default` set's are served next to the bundled skills, scoped to that package, so an agent learns how to work with this package's data from the package itself:

- `get_skill(skill_name, path)` reads any file of a skill by its path, for example `references/joins.md` or `scripts/check.py`. `path` defaults to `SKILL.md`. Reading `SKILL.md` also returns the parsed `frontmatter` and `files`, every file of the skill as `{path, size}`, which is the skill's served set unless the reply says some entries were cut to keep it under the 35,000-character tool-result limit. A binary file answers with its size and a message, never its bytes.
- `GET …/packages/{pkg}/skills` lists the skills and `…/skills/{skill}` reads `SKILL.md`. `content` is the file as written, frontmatter included, with nothing appended. A package skill's other files have one REST read, the set route `GET …/skill-sets/{set}/skills/{skill}/{file}`, and the `default` set's skills are read at `…/skill-sets/default/skills/…`; a bundled skill's other files are read only through `get_skill`. A listed file (not Markdown, or Markdown that was listed) that changed on disk since the package loaded answers 404 until the package reloads; inline Markdown is served from the text read at load.
- The set and workflow reads (`get_skill_set`, `get_workflow`, `…/skill-sets/{set}`, `…/workflows/{name}`) carry the same skills per set, and a `SKILL.md` is served as written there too, byte for byte, with its `name` and `description` also on the entry. A `SKILL.md` therefore has one form everywhere. What a set read adds is whole sets, listed files and a `digest`, and it serves a skill whatever `overrides` says.

Rules for what is served:

- A `default` skill whose `name` matches a bundled skill replaces it for this package only when the `default` set's `overrides` lists the name; otherwise the bundled skill is served. Nothing is merged. Every entry carries an `origin` (`package` or `bundled`) saying which one you are reading. Credible's agent does not replace: it skips such a package skill and keeps the bundled one, so a package that must behave the same in both gives its skill a name no bundled skill has.
- Every file is read through its real path, and one that resolves outside the package is not served, so a link cannot pull in a file from elsewhere on the machine. A link to a Markdown file inside the package is followed, and so is a skill directory or `reference/` folder that is itself a link into the package; any other link is skipped with a warning. A skill set folder that is itself a link out of the package refuses the set, and so every workflow that lists it.
- A skill whose `SKILL.md` cannot be read or is not UTF-8 text, or whose frontmatter `name` differs from its directory name, is skipped with a finding on its set. One with no `description` is served but flagged. A file that breaks a limit is skipped alone, without dropping its skill. Each case is a load warning, and the package still loads.
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
- A set's skills: `get_skill_set` (or `GET …/skill-sets/{set}`) with `skills: "summary"` lists each inline file and, in the full view, each listed file with its size and sha256; `get_workflow` carries the same per listed set.
- The upload: `cred publish --dry-run` lists the files that would be uploaded. That is the upload set, not the skill set.

### Listed files

A skill carries its Markdown inline: `SKILL.md` and every `*.md` file outside `assets/`, at any depth, while they fit the set's 256 KiB inline budget. Every other file of the skill is listed: scripts, templates, data, images, and any Markdown under `assets/`, that does not fit the budget, or that is not UTF-8. Publisher lists each one without its contents, in the set's `assets` (`{relative_filepath, bytes, sha256}`, with the path relative to the set's folder; the summary view leaves `assets` out). It serves a text file on request through the skill-file route (`GET …/skill-sets/{set}/skills/<skill>/<path>`, MCP `get_skill_set` with `skill_path`, or `get_skill` with `path`). A binary file answers with its size and a `message`, never its bytes, so a caller copies it from the package. Listed files never count against the 256 KiB Markdown budget.

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
