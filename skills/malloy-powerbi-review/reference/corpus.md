<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Development corpus

> The 50 public Power BI models this skill was developed and regression-tested against. They are test material rather than a survey: the point of collecting them was to find routing bugs a two-model sample could not, and every widening found some. Listed so any figure quoted in `translate-measures.md` can be re-derived without redistributing anyone's model - nothing from these repositories is committed here.

## How the corpus was selected

Four gates, in this order. Each one is a filter on the previous, so the counts below only make sense read top to bottom.

1. **Discovery.** GitHub code search for `filename:relationships.tmdl`, `filename:model.tmdl` and `filename:database.tmdl` - three axes rather than one, so a single relevance ranking does not decide the corpus. Union: **1,166 distinct repositories**.
2. **License, before cloning.** Kept only MIT, Apache-2.0, BSD, ISC, 0BSD, CC0 and Unlicense. Dropped everything else, including every repository with no license file at all - no license means all rights reserved, not permission by default. **923 of the 1,166 dropped here**: 858 with no license file, 40 NOASSERTION, 15 GPL-3.0, 2 AGPL-3.0, and 8 over a 400MB size cap. The unlicensed long tail is most of what a code search returns, and it is the single reason the published number rests on 50 models rather than several hundred.
3. **Distinctness.** 100 of the 243 surviving repositories were cloned, which was already more distinct models than the target needed. Deduplicating on the SHA-256 of each model's concatenated `tables/*.tmdl` collapsed **52** models that community template repositories republish from each other, leaving 219.
4. **Substance, then spread.** At least 10 **measures**, counted as measures - not user-defined functions, calculation items or calculated columns, which are DAX but are not measures and were once silently summed with them. 82 of the 219 clear that bar. Those are then capped at **3 models per repository**, so no single author sets the shape of the result, and taken round-robin across repositories in sorted order - deterministic, and it spreads authorship rather than alphabetical luck.

**Result: 50 models from 49 repositories** - 47 MIT, 3 Apache-2.0.

## Re-running it

```
git clone --depth 1 https://github.com/<repo> && git -C <repo> checkout <sha>
python3 scripts/classify_measures.py <repo>/<path-to>/definition
```

The SHA is the commit each model was read at. A later commit may hold a different model; that is the point of pinning it.

## The models

`Other` counts the definitions that are DAX but are not measures - calculation items, user-defined functions, calculated columns, calculated tables and RLS role predicates.

| Repository | SPDX | Commit | Model | Measures | Other |
|---|---|---|---|---:|---:|
| `Abhishek-Bangale/Germany-Crime-Analytics-Dashboard` | MIT | `0cec555b8c` | Germany_Crime_Analysis.SemanticModel | 20 | 0 |
| `Adrian-LCardoso/Do-Lancamento-Contabil-a-Tomada-de-Decisao` | MIT | `15bf80b0be` | Vendas.SemanticModel | 23 | 6 |
| `ahmedtareq404/Cocoa_Land_Supply_Chain_Analytics_Project` | Apache-2.0 | `5bf7333329` | CocoaLand.SemanticModel | 45 | 12 |
| `alcoder06/economic-data-warehouse` | MIT | `045257727f` | UzbekistanEconomicAnalysis.SemanticModel | 33 | 0 |
| `alisonpezzott/pbi-docs` | MIT | `2470004d1d` | pbi_docs_report.SemanticModel | 10 | 1 |
| `AmenBouallagui/fabric-sales-marketing-platform` | MIT | `afbba7d497` | SalesMarketing.SemanticModel | 28 | 1 |
| `AmenBouallagui/fabric-sales-marketing-platform` | MIT | `afbba7d497` | SalesMarketingRedesign v2.SemanticModel | 22 | 1 |
| `AndreasTraut/Dynamische-Preisoptimierung` | MIT | `71153ef9ce` | Preisoptimierung.Dataset | 17 | 1 |
| `ankitsharma071/Predictive-Maintenance-Failure-Analytics` | MIT | `23b3876753` | Predictive Maintenance Analytics Dashboard.SemanticModel | 10 | 11 |
| `ArnavMurdande/LLMDEX` | MIT | `0a615f04cb` | LLMDEX Report.SemanticModel | 45 | 9 |
| `AyanMulaskar223/olist-modern-analytics-platform` | MIT | `4276b35c0c` | olist_analytics.SemanticModel | 33 | 7 |
| `bcgov/Mines_Data_CI` | MIT | `69d020fb75` | Gold Incidents Semantic Model.SemanticModel | 27 | 0 |
| `bgarcevic/ripbi` | Apache-2.0 | `0ade9060d8` | AdventureWorks Sales.SemanticModel | 20 | 17 |
| `bhpat/GovernanceOnelens` | MIT | `87135434da` | semantic_model | 19 | 0 |
| `caiorja/Performance-Comercial` | MIT | `f091a8555b` | Performance Comercial.SemanticModel | 35 | 7 |
| `caiorja/Sigma-FastLab` | MIT | `a97f83f959` | SigmaLab.SemanticModel | 37 | 3 |
| `christophermichaelbarber-ctrl/Business-Accelerators` | MIT | `1f82a2bb79` | Profit and Loss Fabric DB.SemanticModel | 86 | 16 |
| `chuahengli/Stock-Portfolio-project` | MIT | `057d232fd1` | Stock Portfolio PowerBI.SemanticModel | 36 | 9 |
| `Contosec-EMU/report-mdo-attack-sim` | MIT | `8ee945a99d` | MDOAttackSimulation.SemanticModel | 27 | 3 |
| `cordialApple/Peekbar` | MIT | `22479304b6` | Logging_shell_real.SemanticModel | 35 | 6 |
| `CSalcedoDataBI/dax-for-agents` | MIT | `1b17ddaacf` | Contoso.SemanticModel | 28 | 3 |
| `CSalcedoDataBI/PowerBI-Deneb` | MIT | `bc51053216` | ContosoRetail.SemanticModel | 15 | 1 |
| `CSalcedoDataBI/powerbi-pbip-tools` | MIT | `5943964ffc` | DAX User-Defined Functions.SemanticModel | 11 | 7 |
| `DataChant/Trello-Power-BI` | MIT | `250f924c55` | Trello.SemanticModel | 35 | 15 |
| `datpro12345/power-bi-tmdl` | MIT | `11345615b9` | AdventureWorks Report_FINAL.SemanticModel | 39 | 21 |
| `dnsamit/Supply-Chain-Visibility-System-with-Optimization-Analytics` | MIT | `f50a980af2` | Supply chain visualization.SemanticModel | 72 | 0 |
| `E-W-Framework-Analysis-Tool/E-W-Indicator-Analytics` | Apache-2.0 | `40d4a8fc1b` | EQ-12.SemanticModel | 60 | 9 |
| `ecotte/Fabric-Monitoring-RTI` | MIT | `2cd63743c4` | Gateway Monitoring.SemanticModel | 21 | 1 |
| `EdwinNRM/AtlasAgroindustrial` | MIT | `822ada43e1` | atlas_controladoria.SemanticModel | 142 | 3 |
| `eric-data-ai/amazon-voc-pipeline` | MIT | `056ca8d9b5` | MarketPlaceAnalysis_Infrared.SemanticModel | 73 | 3 |
| `erickcmendes/CEUB` | MIT | `3bb6e6b6a1` | Projeto_BolsaFamilia+RLS.SemanticModel | 12 | 6 |
| `EvaluationContext/daxlib.svg` | MIT | `7740816119` | SVG.SemanticModel | 30 | 83 |
| `FabricTools/pbir-samples` | MIT | `2713c2ed31` | Competitive Marketing Analysis.SemanticModel | 44 | 10 |
| `fgarofalo56/csa-inabox` | MIT | `9097f0b07c` | retail-sales.SemanticModel | 15 | 0 |
| `FranGenoa/powerbi-tenant-catalog` | MIT | `2c52076c59` | PBI Tenant Catalog | 72 | 0 |
| `Frank-Ellingsen/North-Sea-Oil-Platform-Drill-Tower-Construction` | MIT | `2f90213d7b` | Drill_Tower_EVM_PowerBI_rep.SemanticModel | 60 | 14 |
| `Frank-Ellingsen/Project-Mangagement` | MIT | `e51c5bc1b4` | project_wessels.SemanticModel | 17 | 0 |
| `gdorovin/Chat-with-your-data` | MIT | `73af819c1b` | sm_o2c | 37 | 0 |
| `gethynellis/PL-300-PowerBI-Data-Analysis-Course-Demos` | MIT | `be6510de93` | GitHub - Education Demo.SemanticModel | 17 | 12 |
| `gpn-64/Cortonis-Pharma-Sales-Dashboard` | MIT | `cccd232ce3` | Cortonis Sales Dashboard.SemanticModel | 56 | 19 |
| `gpn-64/Post-LOE-Generic-Access-and-Pricing-in-Canada` | MIT | `06b544cc37` | Post-LOE Generic Access and Pricing in Canada.SemanticModel | 15 | 5 |
| `guilhermefrrr/ucl2425` | MIT | `4a9231a9cd` | pbipucl2425.SemanticModel | 28 | 3 |
| `GuttoF/powerbi-sales-example` | MIT | `c375289a3d` | Sales.SemanticModel | 52 | 1 |
| `Guust-Franssens/tableau-to-powerbi-migration` | MIT | `5bd49f4674` | AirlineAllianceActivity.SemanticModel | 108 | 1 |
| `Harthik777/rbi_neft_graphs` | MIT | `954409a5fe` | RBI-Payments-Intelligence.SemanticModel | 23 | 0 |
| `ilknurdenizozturk/football-player-performance-analysis` | MIT | `5c9871de08` | Model | 43 | 2 |
| `INOPIAE/inoPBI` | MIT | `a9f6e68ae2` | Nordwind.Dataset | 10 | 6 |
| `InsightfulAnalytics/Deneb` | MIT | `32d6c5a127` | Deneb Template Showcase.SemanticModel | 85 | 27 |
| `jaquelinesfernandes/JSTechStore` | MIT | `404184ca20` | JSTechStoreBrasil.SemanticModel | 19 | 0 |
| `JaswanthRamN/Sales-Performance-Revenue-Analytics-Dashboard` | MIT | `a065b13eb0` | SalesPerformance.SemanticModel | 34 | 1 |

## What the corpus is not

- **Not a random sample of Power BI models.** It is a sample of models their authors chose to publish under a permissive license on GitHub, which skews to templates, demos and tooling rather than to a company's working reporting estate.
- **Not a sample of enterprise scale.** The largest model here is well short of the thousands of measures a large Power BI shop carries.
- **Not evidence about report layout.** Only the model is read. Whether a report ever filters an overwritten column lives in `report.json`, which this skill does not read - which is why the routing is a priority order rather than a verdict.
- **Not a license review of your own model.** These are the terms under which the *evidence* may be published, not advice about a customer file.
