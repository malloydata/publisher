# Authoring skills

How skills are structured in this repository, how they reference each other, how manifests
compose them for a use case, and how they reach different hosts. For the why behind the
shape of a skill, read [design principles](./design-principles.md) first.

## Repository layout

```
skills/
  malloy-model/
    SKILL.md
    reference/
      access-modifiers.md
      ...
  malloy-gotchas-modeling/
    SKILL.md
manifests/
  modeling-ide.json
  modeling_and_analysis-ide.json
  ...
```

`skills/` is a flat collection of self-contained skills. A skill has no knowledge of which use
case it belongs to. `manifests/` defines skill compositions for each use case and tells the
host how to deploy them.

## A skill

Each skill is a directory `skills/<skill-name>/` with a `SKILL.md` and any supporting files
under `reference/`.

`SKILL.md` opens with YAML frontmatter, then the body:

```
---
name: malloy-model
description: Author Malloy semantic models: sources, dimensions, measures, joins, access modifiers.
---

Body in Markdown.
```

The `description` is what a host reads on initialization to decide when the skill is relevant,
so write it as a precise trigger, not a summary. Keep the frontmatter valid YAML; a stray
unquoted colon in `description` will break the parse (quote the value if it contains a colon).
The repository's tests parse every `SKILL.md` frontmatter, so a malformed header fails CI.

Keep the body lean. Carry prevention content (the guidance that prevents errors the agent
cannot self-correct) and leave reference content to on-demand lookups. Put long reference
material in `reference/` files the body points to, rather than inline.

Keep a skill self-contained: do not reference the use case or manifest it belongs to, and do
not prescribe which skill loads next (Principle 1). An orchestrating skill may propose a
sequence and point each step at a focused skill; a focused skill must not name a follow-up.

## Cross-skill references

Refer to another skill with the `skill:` prefix, for example `skill:malloy-model` or
`skill:malloy-gotchas-modeling`. This is a logical name, not a file path. The host resolves it to
wherever that skill lives in the deployed layout. Reference-file paths inside a skill should
be relative to the skill, for example `reference/access-modifiers.md`, not an absolute or
host-specific path.

## Manifests

A manifest groups skills into a use case and tells the host how to deploy them.

```json
{
  "name": "modeling-ide",
  "description": "Skills for the Malloy semantic-modeling workflow in external IDEs and CLIs.",
  "trigger_hint": "when the user asks about modeling data, building a semantic model, creating Malloy sources, or mentions Malloy",
  "auto_discovered": ["malloy-modeling"],
  "supporting": ["malloy-model", "malloy-define", "gotchas-modeling"]
}
```

The skill names above are illustrative. In a real manifest, use the actual directory names under
`skills/`.

- `auto_discovered`: skills the host places where it discovers them automatically (the primary
  workflow skills to load when relevant).
- `supporting`: skills the host keeps on-demand, read when an auto-discovered skill directs the
  agent to them.
- `trigger_hint` (optional): text the host uses to generate rule files for IDEs that drive
  skill loading from rules; falls back to `description` if omitted.

How a host maps `auto_discovered` versus `supporting` onto disk (which directory, which rule
file) is the host's concern, not this repository's. The repository serves content and manifest
metadata; directory conventions live in the host, so a new host or editor only needs host-side
changes.

## Reaching different hosts

Skills reach hosts through two channels, and they do not have equal fidelity.

- Native skill files. Skill-aware hosts read the `SKILL.md` files directly. This is full
  fidelity: descriptions load on initialization, prevention content can be always-on, and the
  manifest's auto-discovered and supporting split is honored.
- MCP prompts and resources. Hosts that surface MCP but do not load skill files the same way
  reach the same content as MCP prompts and resources, which are on-demand by nature.

The consequence for authoring: always-on prevention content degrades to on-demand on a host
that only has the MCP channel, which weakens its first-pass-correctness purpose. When a piece
of prevention guidance is load-bearing (a syntax gotcha that otherwise causes a cascading
error), make sure it is also reachable on demand and worth surfacing as such, rather than
assuming it is always loaded.

## Authoring a new skill

1. Create `skills/<skill-name>/` with a `SKILL.md` and any `reference/` files.
2. Keep the skill self-contained: no reference to its use case or manifest.
3. Use `skill:<other-skill>` to point at other skills.
4. Add the skill to the relevant manifest(s) under `auto_discovered` or `supporting`.
5. Cut a release per the repository's release mechanism (the host serves content at a tagged
   version).
