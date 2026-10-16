// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Alert, Box, Stack, Typography } from "@mui/material";
import { useEffect, useMemo, useRef, useState } from "react";
import type { DashboardManifest } from "../../client";
import { useCompiledDocument } from "../../hooks/useCompiledDocument";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { DashboardTile } from "../Dashboard/DashboardTile";
import {
   TileCard,
   TileHeading,
   type TileChrome,
   type TileHeadingSlots,
} from "../Dashboard/TileCard";
import { tileIgnoredFilterLabels } from "../Dashboard/TileFilterTag";
import { documentPreamble } from "../Dashboard/textSource";
import { GivensPanel } from "../given";
import { Loading } from "../Loading";
import { TILE_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import { useReportListing } from "../Package/useDocumentLocation";
import {
   BUILDER_HANDOFF_STALE_MS,
   reportQueryOptions,
} from "../Report/reportQuery";
import { useServer } from "../ServerProvider";
import { ReportBuilder } from "./ReportBuilder";
import {
   missingRequiredGivens,
   requiredPrompt,
   waitingOn,
   waitingText,
} from "../given/required";
import type { FixedFilter } from "./FilterStrip";
import type { ReportLayout } from "../Package/documentLocation";
import {
   isQueryTile,
   tileKey,
   type DashboardDocument,
   type DashboardTile as DocumentTile,
   type QueryTile,
} from "./document";
import type { SavesTo } from "./documentSession";
import {
   givensReadThroughModel,
   previewGivens,
   previewTileQuery,
   tileExpressionKey,
   type PreviewTileQuery,
} from "./preview";
import type { TextSourceOptions } from "./ReportEditor";
import type { ReportEventHandler } from "../Report/telemetry";
import { tileDisplayTitle } from "./tileDisplayTitle";
import type { ViewPreview } from "./AddTileDialog";
import { chartLineText } from "./chartLine";
import { useEditorCatalog } from "./useEditorCatalog";
import type { SaveContext, SaveHandler } from "./useDocumentEditor";
import type { BuilderControlsHandler } from "./builderControls";

/**
 * The builder under a real control row, with the controls driving the tiles,
 * both following the LIVE document rather than the saved file.
 *
 * Owns the given values for the same reason the reader's `Dashboard` does: the
 * bar and the tiles have to read one set of values, so whatever holds them has
 * to sit above both. The manifest supplies what the file cannot say about the
 * model's own givens, and the set of givens the server can be sent; a control
 * declared here and not yet in the package is written into each tile's query as
 * a literal, so it works the moment it is added.
 */
/** A compile with no readable document: no manifest, but the text still opens. */
const NO_MANIFEST: DashboardManifest = {};

export function EditorSurface({
   textSource,
   environmentName,
   packageName,
   modelPath,
   versionId,
   queryVersionId,
   opened,
   replaces,
   onSave,
   onDirtyChange,
   onExit,
   onOpenModel,
   onControlsChange,
   draftKey,
   onEvent,
   savesTo,
   saveLabel,
   note,
}: {
   textSource?: TextSourceOptions;
   environmentName: string;
   packageName: string;
   modelPath: string;
   /** A display label; the editor keys on `modelPath`. */
   slug: string;
   /** The document's version, from its `resourceUri`: what its control state is keyed by. */
   versionId?: string;
   /**
    * The package version every query runs at: tiles, the manifest, filter
    * options, the listing and the catalog. `ReportEditor` passes its
    * `queryVersionId`, or the document's own version when there is none.
    */
   queryVersionId?: string;
   opened: {
      source: string;
      document: DashboardDocument;
      generation: number;
   };
   replaces?: string;
   /** Save the text, with the layout the document has as it is saved. */
   onSave?: (source: string, layout: ReportLayout) => Promise<void>;
   onDirtyChange: (dirty: boolean, wouldLose: boolean) => void;
   onExit?: () => void;
   onOpenModel?: (modelPath: string) => void;
   onControlsChange?: BuilderControlsHandler;
   /** Keep unsaved edits in this browser under this key; see `ReportBuilderProps.draftKey`. */
   draftKey?: string;
   onEvent?: ReportEventHandler;
   savesTo: SavesTo;
   saveLabel?: string;
   note: string;
}) {
   const { apiClients } = useServer();

   // The server serves a dashboard only once the package file has a tile, so
   // an empty start becomes servable when a save puts the first one there.
   const [served, setServed] = useState(opened.document.tiles.length > 0);
   useEffect(
      () => setServed(opened.document.tiles.length > 0),
      [opened.generation, opened.document],
   );

   const {
      data,
      isSuccess: manifestLoaded,
      isError: manifestFailed,
   } = useQueryWithApiError({
      // The reader's read of the same report, so opening the builder from the
      // reader does not read it again.
      ...reportQueryOptions(
         apiClients,
         {
            environmentName,
            packageName,
            path: modelPath,
            versionId: queryVersionId,
         },
         BUILDER_HANDOFF_STALE_MS,
      ),
      // The server does not serve a dashboard with no tiles, so asking would 404.
      enabled: served && !textSource,
   });
   // A text source takes its manifest from compiling the text as the viewer.
   const runModelPath = textSource?.modelPath ?? modelPath;
   const compileText = useCompileText(opened.source, textSource?.compileSource);
   const compiled = useCompiledDocument(
      {
         environmentName,
         packageName,
         modelPath: runModelPath,
         source: compileText.text ?? opened.source,
         ...(textSource?.givens ? { givens: textSource.givens } : {}),
      },
      { enabled: textSource !== undefined && compileText.text !== undefined },
   );
   const compiledDocument = compiled.data?.document;
   const manifest: DashboardManifest | undefined = textSource
      ? compiled.isSuccess
         ? (compiledDocument?.manifest ?? NO_MANIFEST)
         : undefined
      : data;
   const isSuccess = textSource ? compiled.isSuccess : manifestLoaded;
   const isError = textSource
      ? compiled.isError || compileText.problem !== undefined
      : manifestFailed;
   const preamble = textSource
      ? documentPreamble(compiledDocument?.cells)
      : undefined;
   const compileErrors = (compiled.data?.result.problems ?? []).filter(
      (problem) => problem.severity === "error",
   );
   const hidden = useMemo(
      () => new Set(textSource?.hiddenGivens ?? []),
      [textSource],
   );
   const hostGivens = textSource?.givens;
   const restrictedTiles = useMemo(
      () =>
         new Set(
            (manifest?.tiles ?? []).flatMap((tile) =>
               tile.restricted && tile.query
                  ? [tileExpressionKey(tile.query)]
                  : [],
            ),
         ),
      [manifest],
   );

   // The package's reports: the catalog leaves them out, and every OTHER one
   // is where a clicked cell can go.
   const { reports: reportList, settled: reportsSettled } = useReportListing(
      environmentName,
      packageName,
      queryVersionId,
   );
   const otherDashboards = useMemo(
      () =>
         reportList
            .map((r) => r.path)
            .filter(
               (path): path is string =>
                  !!path && path !== modelPath && path.endsWith(".malloy"),
            )
            // A drill names a report by its path without `.malloy`.
            .map((path) => path.replace(/\.malloy$/, "")),
      [reportList, modelPath],
   );

   const catalog = useEditorCatalog({
      environmentName,
      packageName,
      ...(queryVersionId !== undefined ? { versionId: queryVersionId } : {}),
      modelPath,
      reports: reportList,
      reportsSettled,
   });

   // A text-held document has no `import`, so only what its run model lists resolves for it.
   const runSources = useMemo(
      () =>
         textSource
            ? (catalog?.sources ?? [])
                 .filter((s) =>
                    (s.visibleIn ?? s.exporters ?? [s.modelPath]).includes(
                       runModelPath,
                    ),
                 )
                 .map((s) => s.name)
            : undefined,
      [catalog, textSource, runModelPath],
   );
   const offered = useMemo(
      () =>
         catalog && runSources
            ? {
                 sources: catalog.sources.filter((s) =>
                    runSources.includes(s.name),
                 ),
              }
            : catalog,
      [catalog, runSources],
   );

   const [doc, setDoc] = useState(opened.document);
   useEffect(() => setDoc(opened.document), [opened.document]);
   // Only a package write can make the package serve it; a copy in the host's store does not.
   // The layout the document has as it is saved goes with it, so a store records the one a switch left.
   const saveThenServe = useMemo<SaveHandler<DashboardDocument> | undefined>(
      () =>
         onSave &&
         (async (source: string, context: SaveContext<DashboardDocument>) => {
            await onSave(source, context.document.layout);
            // An undone first save writes back a file with no tile, which the server stops serving.
            if (savesTo === "package")
               setServed(context.document.tiles.length > 0);
         }),
      [onSave, savesTo],
   );
   const modelSpecs = useMemo(
      () =>
         (manifest?.givens ?? []).filter(
            // Only a document held as text hides a `#(secure)` given's control; a saved file's givens show as before.
            (spec) =>
               !(textSource && spec.secure === true) &&
               !(spec.name !== undefined && hidden.has(spec.name)),
         ),
      [manifest, hidden, textSource],
   );
   const runnable = useMemo(
      () =>
         new Set([
            ...modelSpecs
               .map((spec) => spec.name)
               .filter((name): name is string => name !== undefined),
            // A host-set given has no control, but the server is sent it.
            ...Object.keys(hostGivens ?? {}),
         ]),
      [modelSpecs, hostGivens],
   );
   // What the saved file's compiled tiles read, keyed as the server keys a tile expression.
   const servedReads = useMemo(
      () =>
         new Map(
            (manifest?.tiles ?? []).flatMap((tile) =>
               tile.kind !== "text" && tile.query && tile.givenNames
                  ? [[tileExpressionKey(tile.query), tile.givenNames] as const]
                  : [],
            ),
         ),
      [manifest],
   );
   // Givens a tile reads through the model (a source's own `where:`), not a
   // binding in this file: shown, so the row matches the reader's, but the
   // model's to change.
   const modelReads = useMemo(
      () => givensReadThroughModel(doc, opened.document, servedReads),
      [doc, opened.document, servedReads],
   );
   const specs = useMemo(
      () => previewGivens(doc, modelSpecs, modelReads),
      [doc, modelSpecs, modelReads],
   );
   // A filter in the row the builder can show and set but not change: say
   // where it is set instead. Not always the model: a report's own `source:`
   // can apply a given to every tile on it with a `where:` of its own.
   const fixedFilters = useMemo(() => {
      const bound = new Set(
         doc.tiles.flatMap((tile) =>
            isQueryTile(tile) ? (tile.filters ?? []).map((f) => f.given) : [],
         ),
      );
      const declared = new Set((doc.localGivens ?? []).map((g) => g.name));
      const models = [...new Set(doc.imports.map((i) => i.from))];
      // The model by its package path, for "Open the model": an import is
      // relative to the report's own folder.
      const modelFile =
         models.length === 1
            ? packagePathOf(modelPath, models[0]!)
            : models.length === 0 && textSource
              ? textSource.modelPath
              : undefined;
      const reason = (name: string): FixedFilter => {
         const scoping = doc.sources
            .filter((source) => source.scopedBy?.includes(name))
            .map((source) => source.name);
         if (scoping.length > 0)
            return {
               reason: `Applied to every tile on ${scoping.join(", ")} by this report's own source. Edit the file to change it.`,
            };
         if (declared.has(name))
            return {
               reason:
                  "Read by a query in this report the builder cannot edit. Edit the file to change it.",
            };
         return modelFile !== undefined
            ? {
                 reason: `Set in the model ${modelFile}. Edit the model to change it.`,
                 modelPath: modelFile,
              }
            : {
                 reason:
                    "Set in a model this report imports. Edit the model to change it.",
              };
      };
      return new Map(
         [...modelReads]
            .filter((name) => !bound.has(name))
            .map((name) => [name, reason(name)]),
      );
   }, [doc, modelReads, textSource, modelPath]);
   const { declaredTypes, applied, panel } = useDocumentControls({
      specs,
      loaded: isSuccess,
      startingValues: manifest?.startingGivens,
      // Keyed by path: two reports may share a basename.
      documentKey: `${environmentName}/${packageName}/${versionId ?? ""}/${modelPath}/edit`,
      autorun: manifest?.autorun !== false,
      environmentName,
      packageName,
      modelPath: textSource ? runModelPath : manifest?.path,
      versionId: queryVersionId,
      documentName: modelPath,
      ...(preamble !== undefined ? { preamble } : {}),
      ...(hostGivens ? { hostGivens } : {}),
   });
   // Required filters with no value yet: as in the reader, a tile that
   // reads one waits, and the page asks for them.
   const missing = useMemo(
      () => missingRequiredGivens(specs, applied, hostGivens),
      [specs, applied, hostGivens],
   );
   // A given only a gate reads has no control, so the host's value is added; each tile then sends just the names it reads.
   const tileHostGivens = useMemo(
      () =>
         Object.fromEntries(
            Object.entries(hostGivens ?? {}).filter(
               ([name]) => !declaredTypes.has(name),
            ),
         ),
      [hostGivens, declaredTypes],
   );

   const manifestSettled = !served || isSuccess || isError;
   // Held text that did not compile, or that the host's `compileSource`
   // refused: nothing declares its sources, so a tile run would only fail with
   // "Reference to undefined object". Its tiles are drawn but never run; the
   // problem itself is said once, above the page.
   const notCompiled =
      textSource !== undefined &&
      (compileText.problem !== undefined ||
         compiled.isError ||
         compileErrors.length > 0);
   // The tiles as the file was opened, by key: a tile still equal to its
   // opened self runs exactly as the reader runs it (below).
   const openedTiles = useMemo(
      () =>
         new Map(
            opened.document.tiles.map((tile) => [tileKey(tile), tile] as const),
         ),
      [opened.document],
   );
   const renderTile = useMemo(
      () =>
         function LiveTile(
            tile: QueryTile,
            heading?: TileHeadingSlots,
            chrome: TileChrome = "card",
         ) {
            // The bindings a tile runs with come from the manifest; running before it lands queries every tile once unbound and again bound.
            // Until then the tile's own card and heading stand, so nothing pops in when it runs.
            if (notCompiled)
               return (
                  <TileCard chrome={chrome}>
                     <TileHeading
                        title={heading?.title ?? tileDisplayTitle(tile)}
                        subtitle={heading ? heading.subtitle : tile.subtitle}
                     />
                     <Typography
                        variant="body2"
                        sx={{ color: "text.secondary", py: 1 }}
                     >
                        Not run: this document did not compile.
                     </Typography>
                  </TileCard>
               );
            if (!manifestSettled)
               return (
                  <TileCard chrome={chrome}>
                     <TileHeading
                        title={heading?.title ?? tileDisplayTitle(tile)}
                        subtitle={heading ? heading.subtitle : tile.subtitle}
                     />
                     <Loading text="Running…" />
                  </TileCard>
               );
            const servedReadsOf = servedReads.get(
               tileExpressionKey(`${tile.source} -> ${tile.name}`),
            );
            // A tile not yet edited runs the served view with the served
            // tile's givens: the request the reader sends, so it shares the
            // reader's cached result and opening the builder re-runs nothing.
            // The preview rebuilds the query from the document so an edit
            // shows before it is saved, which an untouched tile does not need.
            const untouched =
               preamble === undefined &&
               servedReadsOf !== undefined &&
               sameTile(openedTiles.get(tileKey(tile)), tile);
            const query: PreviewTileQuery = untouched
               ? {
                    expression: `${tile.source} -> ${tile.name}`,
                    givenNames: [...servedReadsOf],
                    reads: [...servedReadsOf],
                 }
               : previewTileQuery(doc, tile, runnable, applied, servedReadsOf);
            return (
               <DashboardTile
                  environmentName={environmentName}
                  packageName={packageName}
                  versionId={queryVersionId}
                  modelPath={runModelPath}
                  {...(preamble !== undefined ? { preamble } : {})}
                  {...(restrictedTiles.has(
                     tileExpressionKey(`${tile.source} -> ${tile.name}`),
                  )
                     ? { restricted: true }
                     : {})}
                  tile={query.expression}
                  {...(query.annotation
                     ? { annotation: query.annotation }
                     : {})}
                  label={tileDisplayTitle(tile)}
                  chrome={chrome}
                  subtitle={tile.subtitle}
                  {...(heading ? { heading } : {})}
                  borderless={tile.borderless}
                  givens={applied}
                  hostGivens={tileHostGivens}
                  declaredTypes={declaredTypes}
                  givenNames={query.givenNames}
                  {...(() => {
                     const waiting = waitingOn(
                        missing,
                        query.reads ?? query.givenNames,
                     );
                     return waiting.length > 0
                        ? { waitingFor: waitingText(waiting) }
                        : {};
                  })()}
                  ignoredFilters={tileIgnoredFilterLabels(query.reads, specs)}
                  height={TILE_MAX_HEIGHT}
               />
            );
         },
      [
         doc,
         openedTiles,
         manifestSettled,
         notCompiled,
         missing,
         runnable,
         servedReads,
         environmentName,
         packageName,
         queryVersionId,
         runModelPath,
         preamble,
         restrictedTiles,
         applied,
         tileHostGivens,
         declaredTypes,
         specs,
      ],
   );

   // A view picked in Add a tile, run as the tile it would add: against the
   // model that exports its source, with the page's filters, so the preview is
   // the tile as it will look here.
   const renderViewPreview = useMemo(
      () =>
         function ViewPreviewTile(view: ViewPreview) {
            return (
               <DashboardTile
                  environmentName={environmentName}
                  packageName={packageName}
                  versionId={queryVersionId}
                  modelPath={view.modelPath}
                  tile={`${view.base} -> ${view.view}`}
                  {...(view.chart
                     ? {
                          annotation: chartLineText(
                             view.chart,
                             view.chartCarried,
                          ),
                       }
                     : {})}
                  label={view.label}
                  borderless
                  givens={applied}
                  hostGivens={tileHostGivens}
                  declaredTypes={declaredTypes}
                  givenNames={view.givens}
                  height={200}
               />
            );
         },
      [
         environmentName,
         packageName,
         queryVersionId,
         applied,
         tileHostGivens,
         declaredTypes,
      ],
   );

   return (
      <Stack sx={{ gap: 1 }}>
         {textSource && compileErrors.length > 0 && (
            <Alert severity="error">
               The server could not compile this document:{" "}
               {compileErrors.map((problem) => problem.message).join("; ")}
            </Alert>
         )}
         {missing.length > 0 && !notCompiled && (
            <Alert severity="info">{requiredPrompt(missing)}</Alert>
         )}
         {textSource && compileText.problem !== undefined && (
            <Alert severity="error">
               This document could not be prepared to compile:{" "}
               {compileText.problem}
            </Alert>
         )}
         {textSource && compiled.isError && (
            <Alert severity="error">
               The server could not compile this document:{" "}
               {compiled.error?.message}
            </Alert>
         )}
         <ReportBuilder
            {...(textSource && !textSource.compileSource
               ? {
                    addFilterDisabledReason:
                       "This document holds no given: of its own. Bind a filter the model offers from its chip.",
                 }
               : {})}
            source={opened.source}
            document={opened.document}
            fixedFilters={fixedFilters}
            {...(textSource?.takenGivenNames
               ? { takenGivenNames: textSource.takenGivenNames }
               : {})}
            renderTile={renderTile}
            renderViewPreview={renderViewPreview}
            givens={specs
               .filter((spec) => spec.name !== undefined)
               .map((spec) => ({
                  name: spec.name as string,
                  ...(spec.label ? { label: spec.label } : {}),
                  ...(spec.type ? { type: spec.type } : {}),
                  ...(spec.suggest?.dimension
                     ? { field: spec.suggest.dimension }
                     : {}),
               }))}
            onChange={setDoc}
            onDirtyChange={onDirtyChange}
            {...(onExit ? { onExit } : {})}
            {...(onOpenModel ? { onOpenModel } : {})}
            {...(onControlsChange ? { onControlsChange } : {})}
            {...(draftKey !== undefined ? { draftKey } : {})}
            {...(offered ? { catalog: offered } : {})}
            dashboards={otherDashboards}
            {...(onEvent ? { onEvent } : {})}
            controls={
               isSuccess ? (
                  // The reader's control row: filter buttons, in either layout.
                  <GivensPanel {...panel} layout="bar" />
               ) : undefined
            }
            {...(saveThenServe ? { onSave: saveThenServe } : {})}
            savesTo={savesTo}
            {...(saveLabel ? { saveLabel } : {})}
            {...(replaces !== undefined ? { replaces } : {})}
            modelPath={modelPath}
            {...(textSource ? { visibleSources: runSources } : {})}
         />
         <Box sx={{ px: 0.5 }}>
            <Typography variant="caption" sx={{ opacity: 0.7 }}>
               {note}
            </Typography>
         </Box>
      </Stack>
   );
}

/** Whether a tile runs as its opened self did: same view, bindings and chart ({@link whatRuns}). */
function sameTile(
   opened: DocumentTile | undefined,
   now: DocumentTile,
): boolean {
   return (
      opened !== undefined &&
      JSON.stringify(whatRuns(opened)) === JSON.stringify(whatRuns(now))
   );
}

/**
 * A tile less what only frames its result: its title, subtitle, width, row
 * and card are drawn by the tile around the result, not sent with the query.
 * Retitling a tile re-ran it before, and its chart blinked as the same rows
 * were drawn again.
 */
function whatRuns(tile: DocumentTile): DocumentTile {
   const {
      label: _label,
      subtitle: _subtitle,
      colspan: _colspan,
      break: _break,
      borderless: _borderless,
      ...runs
   } = tile as DocumentTile & {
      label?: unknown;
      subtitle?: unknown;
      colspan?: unknown;
      break?: unknown;
      borderless?: unknown;
   };
   return runs as DocumentTile;
}

/**
 * The text a held document is compiled from: its own, or what the host's
 * `compileSource` makes of it. Read once per opened text, so a host passing
 * a new function each render does not recompile.
 */
function useCompileText(
   source: string,
   compileSource: ((text: string) => string | Promise<string>) | undefined,
): { text?: string; problem?: string } {
   const transform = useRef(compileSource);
   transform.current = compileSource;
   const hosted = compileSource !== undefined;
   const [state, setState] = useState<{
      from: string;
      text?: string;
      problem?: string;
   }>();
   useEffect(() => {
      if (!hosted) return;
      let live = true;
      Promise.resolve()
         .then(() => transform.current!(source))
         .then(
            (text) => {
               if (live) setState({ from: source, text });
            },
            (error: unknown) => {
               if (live)
                  setState({
                     from: source,
                     problem:
                        error instanceof Error ? error.message : String(error),
                  });
            },
         );
      return () => {
         live = false;
      };
   }, [source, hosted]);
   if (!hosted) return { text: source };
   // Until this text is resolved, nothing: never the previous text's result.
   if (state?.from !== source) return {};
   return state.problem !== undefined
      ? { problem: state.problem }
      : { text: state.text };
}

/** An import's path (`../storefront.malloy`) as a package path, from the importing file's own path. */
function packagePathOf(fromFile: string, imported: string): string {
   const parts = fromFile.split("/").slice(0, -1);
   for (const part of imported.split("/")) {
      if (part === "..") parts.pop();
      else if (part !== "." && part !== "") parts.push(part);
   }
   return parts.join("/");
}
