<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->
# The five archetypes

> Expanded from `SKILL.md` §2. Read when you have picked an archetype and need to know what it actually looks like, or when you are deciding between two that seem close.

Each entry below gives the job, what leads the page, the depth that belongs, and the failure mode that turns it back into a generic dashboard. Where the arrangement itself is the point, there is a layout sketch: those are shapes, not templates, so copy the hierarchy rather than the box count.

Anything below that says "drawer", "drill", or "cross-filter" has its mechanics in `reference/depth-patterns.md`; read that before building one.

## Monitor

**Job:** is anything wrong right now? Opened often, read in seconds, usually not read at all when things are fine.

**Leads with:** state, not level. "3 breaching" beats a chart of the metric. Deltas and thresholds, not raw numbers.

**Depth:** shallow on purpose. Drill into an anomaly and nothing else. Linkable state so an alert can point at the exact view.

**Fails when** it grows sections. Each added section costs scan time, which is the only thing a monitor sells. If the reader has to look for the problem, it has stopped being a monitor.

## Scorecard

**Job:** are we hitting the target? Read on a cadence, by someone accountable for the number.

**Leads with:** variance to target, sorted so the misses are at the top. An alphabetical metric list buries the thing the reader came for.

**Depth:** drill from a miss to its drivers - the split by the one dimension that explains it. Name that dimension per metric while designing; if there is no useful split, say so rather than inventing one.

**Fails when** every metric gets equal weight and the reader has to compute variance themselves. A scorecard whose rows are just current values is a KPI grid.

## Explorer

**Job:** what is going on in this population, and which entities deserve attention? Used by an analyst who arrives with a question the app's author did not anticipate.

```
[ scope bar: active filters as removable chips        ]
[ ranked list: the entities, by the measure that matters ]
[ distribution: where this entity sits among the rest    ]
[ click any row -> entity drawer: profile, trend,        ]
[   composition, add-to-comparison                       ]
[ comparison tray: collected entities, side by side      ]
```

**Leads with:** the ranked population, not a total. A total is the one number an explorer's user already knows.

**Depth:** deep, and this is where it is the point. Drill-down, entity detail, comparison, cross-filtering, all backed by parameterised detail sources so every panel inherits one definition.

**Fails when** it is built as a flat page of aggregate charts. If a user cannot get from "this bar is tall" to the rows behind it, they will export to a spreadsheet and you have lost.

## Workbench

**Job:** do the recurring work of a role. Opened daily by the same person, who knows exactly where everything is.

```
[ sidebar nav ][ shared controls: period, unit, as-of     ]
[ one page     ][ page content, dense, tables welcome      ]
[  per subject ][ every page obeys the shared controls     ]
[  area        ][ drill panel over the top for one entity  ]
```

**Leads with:** whatever that page's subject leads with. A workbench has no single front page; the overview is one page among several, not the app.

**Depth:** deep, and specifically *cross-page*: state shared across pages, an entity drawer reachable from anywhere, export, and linkable state so a colleague can be sent one exact view. Density is a feature here - this user wants more on screen, not less.

**Fails when** it is one long scrolling page with headings instead of real navigation, or when each page invents its own period control. Shared controls that do not apply everywhere are worse than no shared controls.

## Briefing

**Job:** explain what happened and why, to someone who was not watching. Read once, start to finish.

```
[ the headline finding, in a sentence                 ]
[ the one number that carries it, large               ]
[ prose: what happened                                ]
[ chart: the evidence for that claim                  ]
[ prose: why, and what was ruled out                  ]
[ chart: the driver                                   ]
[ what to do about it / what to watch                 ]
```

**Leads with:** the conclusion. A briefing that opens with methodology has buried its point.

**Depth:** shallow. Link out to an explorer or workbench for anyone who wants to dig; do not build the digging into the briefing itself.

**Fails when** it becomes a dashboard with captions. The test: remove the prose. If the page still makes the same argument, the prose was decoration and the form should have been something else; if the argument collapses, the prose is doing its job.

## Choosing between two that seem close

| If you are torn | Ask | Decide by |
|---|---|---|
| Monitor vs scorecard | Is the reader checking for problems, or reporting against a commitment? | Problems -> monitor. Commitment -> scorecard. |
| Scorecard vs explorer | Is the metric list fixed and agreed, or is the question open? | Fixed -> scorecard. Open -> explorer. |
| Explorer vs workbench | One population, or several subject areas? | One -> explorer. Several -> workbench. |
| Workbench vs several apps | Do the pages share controls and a user, or just a data source? | Share a user -> one workbench. Share only data -> separate apps. |
| Anything vs briefing | Will this be read once, or repeatedly? | Once -> briefing. Repeatedly -> the other one. |

When two genuinely apply to different audiences, that is two apps, not one app with a mode switch. The compromise app serves neither, and it is the most common way a good brief still produces a generic result.
