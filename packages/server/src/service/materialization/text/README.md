# `materialization/text/`

Reads and writes Malloy source text without knowing what materialization
means: a statement-level lexer, readers that place a declaration or a
definition in a file, the edits that blank or rewrite one, and the source map
of a compiled model. Every reader that cannot place what it is asked for
returns `undefined`, never a guess, and generated text is written through
`service/malloy_text.ts`'s emitters.

## Modules

| Module                 | Responsibility                                                                                                           | Read by                                                                                                                                                   |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `malloy_lexer.ts`      | A lexer over Malloy text that knows brackets, words, strings and comments; its token API is internal to the text readers | every module here; `plan/` (the build file generator, cross-connection joins, the rollup measure grammar); `serve/` (the served copy and its containment) |
| `declaration_text.ts`  | Where a top-level source declaration's name ends and its trailing `extend { … }` run begins                              | the build file generator, the re-root rewriter, `kept_ancestors.ts`                                                                                       |
| `definition_cut.ts`    | Where one definition lies in a file, with its annotations, so a rewrite can leave it out                                 | the cross-connection join blanking (`plan/cross_connection_joins.ts`), `inlined_join_text.ts`                                                             |
| `inlined_join_text.ts` | The text that stands an inlined join_one's stored columns in for the join                                                | the re-root rewriter, a `storage=` extension table's chain text                                                                                           |
| `kept_ancestors.ts`    | The bases whose trailing `extend` runs a re-root keeps                                                                   | the build file generator, the served copy                                                                                                                 |
| `field_notes.ts`       | A compiled field's annotation lines, written again on a field Publisher generates                                        | the rollup synthesis, the served copy, the rollup and view legs                                                                                           |
| `persist_blanking.ts`  | The author's files with every source-level `#@` note blanked, by the compiler's spans                                    | the persisted-view and rollup companions, the declaring-file variant, the rollup leg                                                                      |
| `source_map.ts`        | One read-only description of a model's sources, and the one walk of a source's chain                                     | plan, run and serve; `model.ts` and `query_serving.ts`                                                                                                    |

## Dependency rule

May import: itself, `domain/`, `governance/ir`, `service/malloy_text.ts`,
`service/overlay_key.ts`, `service/annotation_reader.ts` (rule R6). It reads
no file and compiles nothing of its own: a caller hands it the text or the
file reader. It reads the compiled IR only through `governance/ir`.

## Where things are added

A new rewrite is a text edit here, written through `malloy_text.ts`; a new
reader of a declaration's shape goes beside `declaration_text.ts`, on the
lexer, so two writers never disagree about which text a statement holds.
