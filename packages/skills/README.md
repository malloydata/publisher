<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# @malloy-publisher/skills

## Start here

1. **Learn.** Read the getting-started skill. Nothing to install, no server:

   <https://unpkg.com/@malloy-publisher/skills@latest/skills/malloy-getting-started/SKILL.md>

   Every skill reads the same way. Put its name in place of `malloy-getting-started`.

2. **Install.** If your agent has a shell, copy every skill to where it loads them:

   ```bash
   npx -y @malloy-publisher/skills install           # into this project
   npx -y @malloy-publisher/skills install --global  # into your home directory
   npx -y @malloy-publisher/skills list              # every skill, with a description
   ```

   `install` picks the agent from what it finds. A `CLAUDE.md` means `.claude/skills/`. An
   `AGENTS.md` or `.cursor/` means `.agents/skills/`. With `--global` it looks for `~/.claude/`,
   `~/.agents/`, or `~/.cursor/` instead. If it finds none, it stops and asks you to name one:
   `install claude` or `install agents`.

## What this package is

The agent skills that ship with [Malloy Publisher](https://github.com/malloydata/publisher), as
files you can install. They are task-specific guides for writing Malloy, building and reviewing
models, exploring data, and authoring HTML data apps.

Publisher already serves these skills over MCP, as prompts, including each skill's `reference/`
files. This package exists for the cases MCP does not cover: a host that reads skill files from disk
(Claude Code and the Publisher plugin), a scaffolder that installs them into a new project, or any
tool that wants the files themselves rather than prompt text.

## Use the files from code

```bash
npm install @malloy-publisher/skills
```

## Copy the skills into a project

```js
import { cpSync } from "node:fs";
import { skillsDir } from "@malloy-publisher/skills";

cpSync(skillsDir, ".claude/skills", { recursive: true });
```

Copy the files rather than symlinking them. npm drops symlinks from a tarball, and a symlink into
`node_modules` breaks the moment the tree is pruned or the package is hoisted somewhere else.

## List what is available

```js
import { listSkills } from "@malloy-publisher/skills";

for (const skill of listSkills()) {
   console.log(skill.name, skill.description, skill.dir);
}
```

`listSkills()` reads each skill's frontmatter from disk, so it cannot drift from the files it
describes.

## What is in it

Each skill is a directory holding a `SKILL.md`, and some also carry a `reference/` directory that the
skill points to for detail it does not inline. Start with `malloy-getting-started`. Use
`malloy-modeling` to build or change a model, `malloy-analysis` to explore and answer questions, and
`malloy-review` to check Malloy for correctness.

A few skills (`malloy-modeling`, `malloy-publish`, `malloy-document`, `malloy-getting-started`, and
the `malloy` index) are written for a Publisher host and name Publisher's MCP tools
directly. The rest describe Malloy itself and refer to tools by bare name (`get_context`,
`execute_query`, `search_malloy_docs`), because the prefix depends on the
host. `search_database_schema` maps the same way if a shared skill starts using it. On Publisher those are `get_context`, `execute_query`, `search_malloy_docs`, and
`search_database_schema`.

## Versioning

This package versions on its own line, separately from `@malloy-publisher/server`, `sdk`, and `app`.
A skill edit does not need a server release.

The MCP prompt channel is a separate story: the server compiles the skill bodies into its own bundle
at build time, so installing a newer version of this package does not change what an already-built
server serves over MCP.

## Contributing

The skills live at [`skills/`](../../skills) in the Publisher repo, and this package copies that
directory in when it is packed. Edit them there.
