# `materialization/text/`

Reads and writes Malloy source text without knowing what materialization
means: a statement-level lexer, readers that place a declaration or a
definition in a file, the edits that blank or rewrite one, and the source map
of a compiled model. Every reader that cannot place what it is asked for
returns `undefined`, never a guess, and generated text is written through
`service/malloy_text.ts`'s emitters.

## Modules

| Module                 | Responsibility                                                                                                                                                                                                                                                     | Read by                                                                                                                                                   |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `key_check_text.ts`    | The text of every key check: one Malloy writer for the four model checks, and a stored table's SQL, each writing its kind's NULL rule                                                                                                                              | the build file generator, the rollup planner, the run's stored key check                                                                                  |
| `malloy_lexer.ts`      | The statement lexer the rewriters read the author's files with: a thin layer over the one Malloy lexer core (`service/malloy_lexer_core.ts`, shared with the caller-text reader) that adds brackets and their depth; its token API is internal to the text readers | every module here; `plan/` (the build file generator, cross-connection joins, the rollup measure grammar); `serve/` (the served copy and its containment) |
| `declaration_text.ts`  | Where a top-level source declaration's name ends and its trailing `extend { … }` run begins                                                                                                                                                                        | the build file generator, the re-root rewriter, `kept_ancestors.ts`                                                                                       |
| `definition_cut.ts`    | Where one definition lies in a file, with its annotations, so a rewrite can leave it out                                                                                                                                                                           | the cross-connection join blanking (`plan/cross_connection_joins.ts`), `inlined_join_text.ts`                                                             |
| `declarations_kept.ts` | A file with only some top-level declarations left, and one definition left out, every position kept                                                                                                                                                                | the served copy's chained-build text, the chained build's compile                                                                                         |
| `inlined_join_text.ts` | The text that stands an inlined join_one's stored columns in for the join                                                                                                                                                                                          | the re-root rewriter, a `storage=` extension table's chain text                                                                                           |
| `kept_ancestors.ts`    | The bases whose trailing `extend` runs a re-root keeps                                                                                                                                                                                                             | the build file generator, the served copy                                                                                                                 |
| `field_notes.ts`       | A compiled field's annotation lines, written again on a field Publisher generates                                                                                                                                                                                  | the rollup synthesis, the served copy, the rollup and view legs                                                                                           |
| `persist_blanking.ts`  | The author's files with every source-level `#@` note blanked, by the compiler's spans                                                                                                                                                                              | `generated_files.ts`                                                                                                                                      |
| `generated_files.ts`   | The Compiler port: compiles generated text over a package's author files and an overlay, never running it; reads an author file as the compile reads it; maps a failed companion to the sources it leaves out                                                      | every companion compiler in `plan/` and the build plan, the chained build (`run/`), the served copy and the rollup and view legs (`serve/`)               |
| `package_files.ts`     | The Compiler port over a package's files on disk, through the server's model runtime; kept apart so the load worker never loads it                                                                                                                                 | the build plan on the main thread, the served copy, the rollup and view legs                                                                              |
| `source_map.ts`        | One read-only description of a model's sources, and the one walk of a source's chain                                                                                                                                                                               | plan, run and serve; `model.ts` and `query_serving.ts`                                                                                                    |
| `author_model.ts`      | One view of a compiled author model for code that reads its text: sources by name, a sourceID index, declaration and file text                                                                                                                                     | the build (`run/source_build.ts`, the `storage=` tier), the served copy, storage bindings                                                                 |

## Dependency rule

May import: itself, `domain/`, `governance/ir`, `service/malloy_text.ts`,
`service/overlay_key.ts`, `service/annotation_reader.ts` and
`service/model_runtime.ts` (rule R6). It reads the compiled IR only through
`governance/ir`. Only `generated_files.ts` and `package_files.ts` compile: they are the
module's only makers of a Malloy runtime (`layer_rules.spec.ts` checks it), over the
package's files or a runtime factory a host hands it, and it never runs SQL.
Every other module here is handed its text or its file reader.

## The Compiler port

`GeneratedFiles` is how plan, run and serve compile text they generate. A
caller writes its files into an overlay, keyed as the compiler asks for them,
and compiles an entry file over the author's package; a generated file's
`import` of the author's model resolves because it is served at a URL beside
it, while it never appears among the package's models. A companion that fails
reports what it leaves out through `companionOmissions`, one reason per
companion kind.

## Where things are added

A new rewrite is a text edit here, written through `malloy_text.ts`; a new
reader of a declaration's shape goes beside `declaration_text.ts`, on the
lexer, so two writers never disagree about which text a statement holds.
