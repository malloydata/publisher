<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Excel Discovery (Step 1)

> Establish what kind of file you have, inventory every sheet and classify what it is for,
> find out where its data really comes from, and capture the findings in the conversation.
> Does NOT translate formulas; that is `translate-formulas.md`.

## 1. Establish the input shape

| Found | Shape | Action |
|---|---|---|
| `.xlsx`, `.xlsm` | zip of XML parts | Read directly with `scripts/classify_workbook.py`. Best case. |
| `.xls`, `.xlsb` | binary | Ask the user to re-save as `.xlsx` in Excel. The script exits 2 with an `unreadable` flag. |
| an encrypted workbook | an OLE container, not a zip | Same: ask for an unencrypted save. Same exit. |
| `.csv`, `.tsv` | text | Data only: every formula is already gone. Not a prior-art source; lift it as a table. |
| Google Sheets | | File > Download > `.xlsx` (the script's own advice). |
| a Power BI file (`.pbix`, `.pbip`, TMDL) | not this skill | `skill:malloy-powerbi-review`. |

Confirm with the user: "I found an Excel workbook. Use it as prior art?" A workbook with no
formulas and no pivots is a data file; take it to the modeling workflow, not here.

A workbook that was **not saved by Excel** (written by a library, an export tool or a
script) carries no real cached values. The classifier's **Oracle** section says so
(`translate-formulas.md` section 0): stop and ask for a recalculated save before
promising parity.

## 2. Run the classifier

```
python3 scripts/classify_workbook.py book.xlsx            # Markdown report
python3 scripts/classify_workbook.py classify book.xlsx --json [--secret-cell 'Sheet!B3=VAR' ...]
```

`--secret-cell` names a cell holding a secret and the variable that stands for it: its
value is masked everywhere in the report. The JSON carries customer structure (sheet and
column names, formulas, server names, SQL). It never carries a cell value from a
hidden, `veryHidden` or config-named sheet, but **keep it local**: never paste it into a
PR or a shared doc.

Read the report in this order, and stop at the first section that needs a human:

1. **Security flags** are printed first. A macro workbook, an XLM macro sheet, DDE, ActiveX,
   an embedded credential or a sensitivity label (`MSIP_Label_*` in `docProps/custom.xml`;
   such a workbook never enters a corpus or a PR) is resolved before anything else.
2. **External data** (only when present): the workbook is a cache of a database. Section 5.
3. **Sheets**: the inventory. Section 3.
4. **Oracle**: can the cached values be trusted? `translate-formulas.md` section 0.
5. **Sources**, **Formula regions**, **Routes**: `recover-sources.md` and `translate-formulas.md`.
6. **Code attached**: section 4.
7. **Not read**: what the script did not look at. Quote it in the report (`limitations.md`).

## 3. Inventory and classify each sheet

The **Sheets** table has one row per sheet: state (visible, hidden, `veryHidden`), class,
formula cells and constants counted separately, hidden rows, merged ranges, autofilter. The
class is a heuristic over what the sheet holds and how the rest of the workbook reads it;
confirm it with the user before building on it. `--json` adds `class_reason`.

| Class | The rule the script applied | What to do |
|---|---|---|
| `data` | holds an Excel Table, or a tall block of constants | A source (`recover-sources.md`). Several data sheets are several sources. |
| `lookup` | small, mostly constants, and some other formula reads it as the table argument of `VLOOKUP`, `MATCH`, `XLOOKUP` or `INDEX` | A dimension to join, or a bracket table (`cookbook-lookup-aggregate.md#la8` to `#la10`). |
| `report` | formula-dominated and aggregates other sheets, or the sheet is a pivot table | The deliverable. Every formula region gets a route; every cell gets a parity row (`parity.md`). |
| `input` | few constants, read from elsewhere by absolute reference | One `given:` per cell (`cookbook-scenario.md#sc1`). |
| `calc` | formula-dominated and its regions read each other on the sheet | The model: roll-forwards, recurrences, circularities (`cookbook-scenario.md`). |
| `scratch` | empty or almost empty (under 5 cells) | Ask why it is there; usually leave out. |
| `config` | `veryHidden`, or named `config`, `configuration`, `settings`, `credentials`, `secrets` or `connections` | No values are shown. Ask what it holds; it often holds the keys to the rest. |

On the bundled fixture the classes are: `Data` data, `Ledger` data, `Lookup` lookup,
`Report` report, `Assumptions` input, `Forecast` calc, `MonteCarlo` calc, `_Config` config
(`veryHidden`). The script gets them right on a workbook built to be classified; on a real
one, expect to correct some. Treat a `report` sheet that is mostly constants, or a `data`
sheet with scattered formulas, as a question for the user, not a classification.

For each sheet, write down:

- **What it is for**, in the user's words, and who reads it.
- **Whether it is hidden**, and who hid it. A hidden sheet is not a secret, but it is
  information the visible sheets depend on. A `veryHidden` sheet cannot be unhidden from
  Excel's menus, which is itself a question. **A hidden sheet is out of scope** only when
  nothing reads it: no edge into it in `graph.sheet_edges`, no region on another sheet lists it
  under `reads` or `depends_on`, and its defined names evaluate to `#REF!`. Say that, with the
  evidence, and leave it out. **If something does read it, it is in scope** and has no stanza:
  hidden, `veryHidden` and config-named sheets are masked, so the report carries no values from
  them. Ask the user to unhide it or to provide the data; do not guess it from the formulas that read it.
- **Whether the numbers on it are refreshed** from outside (section 5) or typed.
- **Layout traps**: merged headers, subtotal rows inside the data, rows under the table,
  periods across the columns (`recover-sources.md`).

## 4. Code attached to the workbook

Workbooks call code the cells do not show. A formula whose output comes from code outside
the file has a cached value that is a **snapshot, not an oracle**: parity for it is
"matches as of save" at best. The **Code attached** section counts each mechanism through
its tell, separately, and routes it. Anything that leaves no trace is in `limitations.md`.

| Mechanism (id in the report) | Tell | Route | Do |
|---|---|---|---|
| VBA project (`vba`) | `xl/vbaProject.bin` | C (pure UDF) or X (macro writes) | Ask the user to export the modules (Alt+F11, File > Export). The script never parses the binary. A pure `Public Function` translates once you can read it; a macro that writes cells stays in Excel. |
| VSTO customization (`vsto`) | `docProps/custom.xml` `_AssemblyLocation` or `_AssemblyName` | X | Name the seam: the assembly writes cells and the values are static. Ask for the source. |
| XLM / Excel 4.0 macros (`xlm_macro`) | `xl/macrosheets/`, an `Auto_Open` name | NR, security flag | Never run. Ask the user. |
| ActiveX controls, OLE embeddings (`activex`, `ole_embedding`) | `xl/activeX/`, `xl/embeddings/` | X, security flag | Skipped entirely. |
| DDE link (`dde`) | `<ddeLink>` in `xl/externalLinks/`, or a `cmd|'...'!A0` cell | X, security flag | Never resolved. |
| Add-in function (`xll_udf`) | a bare function name that is not built in, or an `_xll.` prefix, in a workbook with no `vbaProject.bin` | NR | "Function `FOO` is not built in, or defined here: which add-in provides it?" Translate only when the user supplies its logic. |
| VBA user-defined function (`vba_udf`) | the same bare unknown name, in a workbook that carries a `vbaProject.bin` | NR | Counted apart from `xll_udf`, because the likely owner is the VBA, not an add-in. Ask the user to export the VBA modules; a pure UDF then translates (C). The script does not read the VBA, so it cannot confirm which. |
| Unresolved function (`unresolved_function`) | `_xludf.` prefix, or a cached `#NAME?` | NR | The cache is not an oracle. |
| Link into an add-in file (`addin_link`) | an `externalLink` to a `.xla` or `.xlam`, or `[n]!Func(...)` | NR | Never resolve the path. |
| Python in Excel (`python_in_excel`) | `_xlfn._xlws.PY(` in a formula, or `<code>` in `xl/pythonScripts.xml` / `xl/python.xml` | C | The code is pandas; the report extracts it and you translate it by hand. A Python-object result has no scalar oracle. |
| Office.js add-in (`web_extension`) | `xl/webextensions/` (a store id, no code) | NR | Which add-in? |
| Live feeds (`vendor_feed`): `BDP`, `BDH`, `BDS`, `FDS`, `CIQ`, Refinitiv, `STOCKHISTORY`, Smart View `HsGetValue`, TM1 `DBRW`, `SAPGetData`, `EPMRetrieveData`, `XFGetCell`, `EssCell`, Jet `NL` | formula tokens | X | The cache is a dated snapshot. Ask whether the organisation has the vendor's warehouse feed (the data question, section 5). The report's message names the system. |
| `CUBEVALUE`, `CUBEMEMBER` and the other `CUBE*` functions (`cube`) | formula tokens | NR | `power-pivot.md`. |
| `WEBSERVICE`, `FILTERXML` (`webservice`) | formula tokens | X, security flag | Never fetch. |
| Linked data types, `IMAGE` (`rich_data`) | `vm=` cells, `xl/richData/` | NR | The base value is a placeholder. |
| Form controls (`form_control`) | `xl/ctrlProps/` with `fmlaLink` | T | The linked cell is an input: a `given:`. The report's Form controls block (`form_controls` in `--json`) lists each control's type, linked cell, list range, min, max, step and selection by reference, with no caption text. `cookbook-scenario.md#sc10`. |
| Scenario Manager (`scenario_manager`) | `<scenarios>` in the sheet XML | T | A scenario table and a `given:`. |
| Solver (`solver`) | hidden `solver_*` defined names | X | `cookbook-scenario.md#sc9`. |
| Named `LAMBDA` (`lambda_name`) | a defined name that is a `LAMBDA` | C | Inline it at each call site, or make it a dimension or measure when it is row-local. |
| Ribbon callbacks (`custom_ui`) | `customUI/customUI*.xml` with `onAction` | flag | Flag only. |
| Conditional formatting by expression, data validation (`cf_expression`, `data_validation`) | `cfRule type="expression"`, `dataValidations` | flag | A validation list is a free enum for a `given:`. `code_attached.data_validation.entries` lists sheet, range, type and the inline items (first 20) or the list's source range, on visible sheets only; a masked sheet is counted, never listed. |
| Office Scripts, Power Automate, COM add-ins | **no trace** (stored in OneDrive, or in the application) | not detected | If a sheet's data "just appears", ask. |

A handful of these tells are spec-derived, not confirmed against a real Excel file:
`xll_udf`, `unresolved_function`, `addin_link`, `python_in_excel`, `web_extension`, `dde`
and `custom_ui`. The report lists them under "Unconfirmed tells". Their absence is weaker
evidence than the absence of the confirmed ones (`limitations.md`). `form_control` and
`scenario_manager` were spec-derived and are now confirmed against real files.

**What to say when something is NR.** Ask the question the route implies, in the user's
terms, and write the answer into the report:

| Route cause | Ask |
|---|---|
| `OFFSET`, `INDIRECT`, `CHOOSE` used as a reference | "Cell X builds its range from text or an offset. What range does it point at on a normal run, and what changes it?" |
| an add-in or unknown function | "Function `FOO` isn't built into Excel, defined in this workbook or in its VBA. Which add-in provides it, and can you send its logic or its results for a few inputs?" |
| a link to another workbook (`external_ref`, or a defined name that points into one) | "These cells read from `[other workbook]`. Is that file available, and is it the source of record or another report?" (The path is never resolved; what the cells show is a snapshot as of the last link refresh.) |
| a vendor or planning feed | "These cells come from [system]. Does the organisation have the same data in a warehouse we can reach?" |
| `LAMBDA`, `MAP`, `SCAN`, a spill reference (`A1#`) | "What does this formula return, in one sentence, and for which inputs?" |
| a sheet whose data just appears | "How does this sheet get its data? Office Scripts and Power Automate leave nothing in the file." |
| a cached `#NAME?` | "This showed `#NAME?` the last time it was saved. Is that expected?" |

Hard rules about the files themselves: never resolve `externalLinks` or DDE; never open a
macro-enabled workbook to recalculate it without telling the user first; skip
`xl/embeddings/`.

## 5. Decide the data question

If a workbook reads from a database, a file or a service, the sheets are **a cache of that
source**, only as current as the last refresh. The classifier prints an **External data**
section when it finds any of: `xl/connections.xml`, `xl/queryTables/*`, a Power Query
`DataMashup` part in `customXml/`, a web or text query, or a pivot cache with
`cacheSource type="external"`. That last one is not always external data: the pivot's
`external_cache.kind` is `data_model` for the workbook's own Power Pivot model
(`power-pivot.md`, not this section), and `relational`, `olap` or `external` for a pivot
over a real source, which is a data question like any other here. Then ask the user
where next month's numbers come from. Four answers lead to four jobs:

1. **The source is still there.** Point Malloy at it; the workbook was only a cache. The
   clean case, and what the rest of this section serves.
2. **The source is there but nobody has credentials.** A people problem, and a real one;
   raise it now.
3. **There is no source.** Someone pastes an export in, or the workbook is the only copy.
   Lifting the sheets gives a working model today and a stale one next month. Say so
   explicitly, rather than letting it be discovered later.
4. **A vendor or planning system holds it** (a live feed from section 4). Point Malloy at
   the vendor's warehouse feed if the organisation has one; otherwise it is X.

### What the script reads

From `xl/connections.xml`, per connection: whether the password is saved
(`savePassword`); the connection string's server, database, user and driver or provider
(the **password is never emitted**); the command with its `commandType`, which is the
SQL, so it becomes the source's `<conn>.sql(...)`; and any `<parameters>`, a query
parameter bound to a cell, which becomes a `given:`. Also web-query URLs (with userinfo and
token-like query values stripped) and text-query paths (reduced to a file name). From a
Power Query `DataMashup` blob (UTF-16 with a BOM, base64, a length-prefixed inner zip),
only `Formulas/Section1.m`, under the same entry and size caps as the outer file: the
connector calls (`PostgreSQL.Database(host, db)` and so on) and their literal arguments,
nothing else. A computed argument (a variable, a Power Query parameter) is skipped, so
a query built from a parameter yields no host. The M transformation steps are not
translated: the sheet holds Power Query's *output*, and a snapshot runs no M.

### What maps, and what does not

| Excel / M source | Publisher connection | Fields |
|---|---|---|
| `PostgreSQL.Database(host, db)`, `Driver={PostgreSQL...}` | `postgres` | `host`, `port`, `databaseName`, `userName`, `password` |
| `MySQL.Database`, `Driver={MySQL...}` | `mysql` | `host`, `port`, `database`, `user`, `password` |
| `Snowflake.Databases(server, warehouse)` | `snowflake` | `account`, `username`, `password`, `warehouse` |
| `GoogleBigQuery.Database()` | `bigquery` | `defaultProjectId`; the credential is application-default credentials or a key, never from the file |
| `Databricks.Catalogs(host, httpPath)` | `databricks` | `host`, `path`, `token`, `defaultCatalog` |
| Trino or Presto over ODBC | `trino` | `server`, `port`, `catalog`, `schema`, `user`, `password` |
| `Excel.Workbook(File.Contents(...))`, `Csv.Document`, a text or Parquet file | DuckDB `read_xlsx` / `read_csv` | the user supplies the file |

Field names are those of `api-doc.yaml` (`PostgresConnection`, `SnowflakeConnection`,
and so on). The script can also say `missing required fields, add them to the block by
hand`: the demo workbook's Snowflake query had no `username`, and the report said so.

**What does not map (NR, with the reason, first in the report).** **SQL Server leads this
list**, because it is the most common finance source and Publisher has no SQL Server
connection type: `SQLOLEDB`, `SQLNCLI*`, `MSOLEDBSQL`, `Sql.Database`. After it come Oracle,
Teradata, SAP HANA, DB2, SSAS and `MSOLAP`, Access, SharePoint lists, `Odbc.DataSource("dsn=...")`
(a DSN is machine-local), Redshift, and integrated or Windows authentication
(`Integrated Security=SSPI`, `Trusted_Connection`). For SQL Server, offer the real options:
a replica or export in a supported warehouse, or DuckDB over an extract. Do not pretend.
An embedded Power Pivot connection (`Data Source=$Embedded$`) is the workbook's own model, not
a server: the script gives it the status `data_model` and lists it once, outside this list
(`power-pivot.md`). Connections that only feed the model or Power Query from the workbook's
own ranges (`WorksheetConnection_*`, connection types 100 and 102) are `workbook`: not NR and
not a cache of a database.

### `connections` and `run`: the credential never enters the conversation

Power Query does not store credentials in the workbook (they live in the author's
data-source settings), so a missing password is the normal case. When one *is* present (a
`Password=` or `Pwd=` in a connection string, an M literal, a cell), it has been shipping
inside the file, and the report prints a `ROTATE` line: rotate it whatever you do next.

```
python3 scripts/classify_workbook.py connections book.xlsx --config-out conn.json \
    [--secret-cell 'Settings!B3=MALLOY_ORDERS_PASSWORD' ...] [--secrets-out PATH] [--force]
python3 scripts/classify_workbook.py run --secrets PATH -- npx @malloy-publisher/server@latest
```

`connections`:

- writes the proposed `connections` block to `--config-out` with every secret as
  `${MALLOY_<CONNECTION>_PASSWORD}` (or `_TOKEN`); a fragment to merge under an environment
  in `publisher.config.json`, not a loadable config;
- writes secret **values** only to `--secrets-out`, a `0600` file in a `0700` directory.
  The default is `$XDG_CONFIG_HOME/malloy-publisher/<workbook>.env` (else under `~/.config`),
  which is outside any work tree. A path inside a git tree is refused unless
  `git check-ignore -q` says it is ignored. An existing file is refused without `--force`
  (executed: `error: the secrets file already exists: pass --force to overwrite it`);
- lists every variable and where it came from, never a value:
  `MALLOY_SALES_PG_PASSWORD ← xl/connections.xml conn 1`, or `MALLOY_ORDERS_PASSWORD: no value
  in the workbook; add it to the secrets file yourself`;
- takes `--secret-cell 'Sheet!B3=VAR'` for a secret that lives in a cell, binding it to the
  variable `VAR` (which must match `[A-Z_][A-Z0-9_]*`, the only shape Publisher substitutes).
  Executed: `MALLOY_ORDERS_PASSWORD ← cell _Settings!B2`.

`run` reads the file and executes the command with those variables in its environment.
The values never reach argv, `ps` or shell history; there is no `env $(cat file | xargs)`.
The file format is one `NAME="value"` per line (escapes `\\ \" \n \r \t`); the reader also
accepts bare values, `'single quoted'`, `export NAME=...` and `#` comments. Executed:
`run --secrets F -- python3 -c "...print(sorted(k for k in os.environ if k.startswith('MALLOY_')))"`
printed `['MALLOY_SALES_PG_PASSWORD']`.

**Two things about starting the server, both executed on Publisher 0.9.0:**

- **The variables must be supplied on every start.** A `publisher.config.json` that
  references `${MALLOY_SALES_PG_PASSWORD}` and is started without it fails with
  `PUBLISHER_INIT_FAILED ... Environment variable '${MALLOY_SALES_PG_PASSWORD}' is not set in
  configuration file`, on an already-initialized server root too. Lower-case names are
  not substituted at all (the pattern is `${[A-Z_][A-Z0-9_]*}`). `docs/configuration.md` describes a `.env`
  autoload by Bun; the `npx` command runs under Node, where it was not relied on, so use
  `run`.
- **A connection added to the config after the first boot is ignored.** With the
  variable supplied and the block in `publisher.config.json`, an already-initialized
  server root started with `PUBLISHER_READY` and `GET /api/v0/environments/x/connections`
  returned `[]`. Restarting with `--init` re-read the config and the connection appeared
  (the API lists it with `withheldFields: ["postgresConnection.password"]`). So put the
  block in the config **before the first boot**, or restart once with `--init`, which
  wipes `publisher.db` and with it anything created in the UI. Creating the connection
  through the REST API (`POST .../connections/{name}`) works without `--init` but carries
  the secret in the request, which defeats the point; do that yourself in a terminal, not
  through the agent.

**Where a resolved secret lives.** After the first boot Publisher stores the resolved
connection config, plaintext included, in `publisher.db` in the server root (read from
the source, `ConnectionRepository`/`environment_store`, not inspected). That is a second at-rest copy of
the credential. `--init` wipes it. Keep the server root out of any work tree and out of
backups you would not give the credential to.

### Hard rules

- **Never read the secrets file or `publisher.db`.** Not with `cat`, not to "check" it.
- **Never dump `xl/connections.xml`, `customXml/`, `xl/queryTables/` or `xl/externalLinks/`
  raw** (`unzip -p`, `cat`, a hex dump). That is the realistic leak path while debugging
  the script. Use the report.
- **If a secret reaches the transcript anyway** (pasted by the user, shown in a dump,
  printed by a bug), **stop and tell the user plainly**: that credential is now in this
  session's history and logs, and must be rotated. Never repeat the value.
- **`--json` masking is best effort; rotation is the real control.** The script masks cells
  named by `--secret-cell`; cells labelled `password`, `pwd`, `secret`, `token`,
  `api key` and common translations (and the first filled cell beside or below one);
  and values shaped like secrets (`Password=...`, `user:pass@host`, `AKIA...`, `sk-...`,
  `ghp_...`, `xox[bp]-...`, JWTs, long high-entropy strings). It scans defined names and
  comments too. It cannot catch a secret that appears only inside a formula string literal
  with no label or recognisable shape. See `limitations.md` for the other gaps.
- The script prints only the exception *class* on an internal error, never the message, so
  a traceback cannot carry a value. That is why the agent may run `connections` itself;
  the user can still run it in their own terminal, but not through `!`, whose output enters
  the conversation.

### What the proposed source looks like

For a connection with a command, the report prints the proposed source and any
parameter given:

```
- conn 1 "Sales PG" -> postgres connection `sales_pg` (host=pg.example.com, port=5432, ...); credential `${MALLOY_SALES_PG_PASSWORD}`
  - proposed source: sales_pg.sql("""SELECT region, sum(amount) AS amount FROM orders WHERE order_date >= /* TODO given: start_date (workbook cell Report!$B$2); bind it here */ NULL GROUP BY region""")
  - proposed `given: start_date` bound to Report!$B$2
```

The query's `?` is a parameter bound to a workbook cell. The script puts a `NULL` and a
`TODO` comment in its place, so the proposed text compiles but is **not yet right**: replace
the `NULL` with the interpolated given. How a `given:` reaches inside a **warehouse**
`<conn>.sql()` was not tested; only `duckdb.sql()` was (`translate-formulas.md`). Test it on
the real connection before relying on it, and if it fails, apply the given as a `where:` on a
wrapper around the source.

## 6. Capture prior-art notes

Hold a lean routing summary in the conversation, with these sections:

- **Source**: shape (`.xlsx`, `.xlsm`), who saved it (Excel, or a library), the oracle
  verdict, date system, and the `dcterms:modified` stamp.
- **Sheets**: sheet, class, state, and the confirmed purpose.
- **Data question**: external sources found, which map, which are NR, and the answer
  to the question above.
- **Code attached**: mechanisms found, with route.
- **Routes**: counts by T, C, X, NR (from the report), with the NR items and the questions
  asked.
- **Flags**: numbered situations requiring attention (security flags, untrusted
  cache, hardcoded constants and plugs, mixed columns, merged headers, hidden rows).
- **Decisions Made During Discovery**: the input shape accepted, classes corrected, the
  answer to the data question.

No cell values and no secrets. Architecture, flags and decisions only.
