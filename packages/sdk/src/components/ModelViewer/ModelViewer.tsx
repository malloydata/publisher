// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Link } from "@mui/material";
import { useCallback, useMemo, useState, type ReactNode } from "react";
import { useGivensState } from "../../hooks/useGivensState";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { parseResourceUri } from "../../utils/formatting";
import { Loading } from "../Loading";
import { ownSourceDoc } from "./docAnnotations";
import { EmptyRow } from "./EmptyRow";
import {
   ModelViewerExtensionsContext,
   type ModelViewerExtensions,
} from "./extensions";
import {
   requiredFiltersBySource as requiredFiltersOf,
   sourceNameSet,
} from "./explorerModel";
import {
   groupEntitiesBySource,
   type EntitiesByField,
   type FieldEntity,
} from "./fieldEntities";
import { givensForSource, givensInheritedBy } from "./givenUsage";
import { HeaderBar } from "./HeaderBar";
import { isNotFoundOrForbidden } from "./httpStatus";
import { joinAccess, type JoinLink } from "./joinAccess";
import { ModelGivensContext } from "./modelGivensContext";
import { ModelGivensSection } from "./ModelGivensSection";
import { declaredTypesOf, givensRequest, type GivenInfo } from "./modelGivens";
import { sourceCountLabel } from "./modelHeaderPresentation";
import { QueriesGroup } from "./QueriesGroup";
import { SchemaView } from "./SchemaView";
import {
   fieldCount,
   pickDefaultSource,
   toExplorerQueries,
   toExplorerSource,
} from "./schemaModel";
import { accessOf, hasAccessRules, type SourceAccess } from "./sourceAccess";
import { baseSourceName } from "./sourceLineage";
import {
   exportOrder,
   orderPickerSources,
   pickerDescription,
   sourceMetaLine,
} from "./sourcePicker";
import type { SourceNavItem } from "./SourcesDropdown";
import { SourcesNav } from "./SourcesNav";
import { CONTENT_COLUMN_SX, FILE_VIEW_TOP_GAP, FLAT_PANE_SX } from "./surface";
import { MONO_FONT_FAMILY, MONO_FONT_VAR, TEXT_TERTIARY } from "./theme";
import { describeApiError, LoadErrorAlert, serverMessage } from "./ui";
import type { QueryRunEvent, RunContext } from "./useRunQuery";
import { useModelSources } from "./useModelSources";

export interface ModelViewerProps {
   /** The model, as `publisher://environments/{env}/packages/{pkg}/models/{path}`
    *  (`encodeResourceUri`), with an optional `versionId`. */
   resourceUri: string;
   /**
    * The source on screen, for a host that keeps it (in its address, say). With
    * it the viewer is controlled: picking a source calls `onSourceChange` and
    * shows what the host passes back. A name the model doesn't expose shows a
    * not-found state naming the sources it does.
    */
   sourceName?: string;
   /** Called with the source a reader picks. */
   onSourceChange?: (sourceName: string) => void;
   /** Which source to open on when uncontrolled; the model's default otherwise. */
   initialSourceName?: string;
   /**
    * Read and run the files and sources the package's `index.malloy` hides, for a
    * reader the server lets edit the package; anyone else is refused. A file read
    * this way is marked Private on its header.
    */
   includeHiddenFilesAndSources?: boolean;
   /** Givens the host fills on every query (`$GROUPS`, say): never a reader's input. */
   hostGivenNames?: ReadonlyArray<string>;
   /**
    * Given values from the host, typically its URL's query parameters
    * (`useGivenUrlParams`), so a link to the page opens with them applied. Read
    * as the page loads and whenever they change beneath it; names the model
    * doesn't declare are ignored. Without it the values live in the viewer.
    */
   givens?: Record<string, string>;
   /**
    * The applied values whenever they change, with every given name the model
    * declares (`managed`, so the host knows which parameters to remove), for a
    * host that keeps them in its URL. Called once the model has loaded.
    */
   onGivensChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
   /** Show the Malloy of a source's access rules beside their words. */
   showAccessRules?: boolean;
   /** The heading level of the source's name: `h1` where the viewer is the page. */
   headingLevel?: "h1" | "h2";
   /** Controls at the right of the source's heading. */
   headerActions?: ReactNode;
   /** A host's chips on the source's status row. */
   renderSourceStatus?: (sourceName: string) => ReactNode;
   /**
    * A host's fuller record of the model's fields (see {@link FieldEntity}):
    * per-field code where the file's text isn't served, inherited columns, join
    * targets and declarations. The viewer reads the model file's text without it.
    */
   entities?: ReadonlyArray<FieldEntity>;
   /** `entities` is still being read: rows hold their code rather than saying there is none. */
   entitiesLoading?: boolean;
   /** `entities` could not be read: a row whose code would come from them says so. */
   entitiesError?: boolean;
   /**
    * A host's descriptions of the columns each source's table passes through
    * (source name → column → text), for a dimension with no `#(doc)` of its
    * own; shown marked as generated.
    */
   columnDescriptions?: ReadonlyMap<string, ReadonlyMap<string, string>>;
   /** Where a join to a source in another model file opens; without it such a join has no arrow. */
   joinHref?: (target: {
      modelPath: string;
      sourceName: string;
   }) => string | undefined;
   /** Labels every run carries (`QueryRequest.queryMetadata`). */
   queryMetadata?: Readonly<Record<string, string>>;
   /** Called once per run that settles. */
   onQueryRun?: (event: QueryRunEvent) => void;
   /** A result larger than this (bytes) is held behind a warning before it renders. */
   maxResultSize?: number;
   /** What a host adds to the rows and runs (see {@link ModelViewerExtensions}). */
   extensions?: ModelViewerExtensions;
}

const NO_ENTITIES: ReadonlyMap<string, EntitiesByField> = new Map();

/** A model's display name: its file name without `.malloy`. */
function modelDisplayName(modelPath: string): string {
   return (modelPath.split("/").pop() ?? modelPath).replace(/\.malloy$/, "");
}

/**
 * A Malloy model, read-only: the selected source's givens, views, the model's
 * named queries, and the source's measures, calculations, dimensions and joins,
 * under a header whose "2 sources ▾" pill switches between the model's sources.
 * Every view and named query runs when opened, and a given's picker reads its
 * options when opened, never on render, so browsing costs nothing; a dimension
 * the model tags `#(index)` lists its common values.
 *
 * Nothing here writes a value into Malloy text. A run is a view or a query by
 * name, every name is quoted (`malloyIdentifier`), and given values travel as
 * the request's `givens`, which the server binds.
 */
export function ModelViewer({
   resourceUri,
   sourceName: controlledSourceName,
   onSourceChange,
   initialSourceName,
   includeHiddenFilesAndSources = false,
   hostGivenNames,
   givens: hostParams,
   onGivensChange,
   showAccessRules = false,
   headingLevel = "h1",
   headerActions,
   renderSourceStatus,
   entities,
   entitiesLoading = false,
   entitiesError = false,
   columnDescriptions,
   joinHref,
   queryMetadata,
   onQueryRun,
   maxResultSize,
   extensions,
}: ModelViewerProps) {
   const {
      environmentName,
      packageName,
      modelPath = "",
      versionId,
   } = parseResourceUri(resourceUri);
   const monoFont = usePublisherTheme().theme.ui.font.mono;
   const {
      sources: sourceInfos,
      queries: namedQueries,
      sourceText,
      givens: modelGivens,
      access: accessBySource,
      modelDoc,
      hasImports,
      isLoading,
      isError,
      error,
      refetch,
   } = useModelSources(
      environmentName,
      packageName,
      modelPath,
      versionId,
      true,
      includeHiddenFilesAndSources,
   );

   const hostGivens = useMemo(
      () => new Set(hostGivenNames ?? []),
      [hostGivenNames],
   );
   const givens = useMemo<GivenInfo[]>(
      () => modelGivens.filter((g) => !hostGivens.has(g.name)),
      [modelGivens, hostGivens],
   );
   const sources = useMemo(
      () => sourceInfos.map(toExplorerSource),
      [sourceInfos],
   );
   const queries = useMemo(
      () => toExplorerQueries(namedQueries),
      [namedQueries],
   );
   const sourceNames = useMemo(() => sourceNameSet(sources), [sources]);
   const requiredFiltersBySource = useMemo(
      () => requiredFiltersOf(sourceInfos),
      [sourceInfos],
   );

   // Given values the reader sets in the Givens section, kept with the model
   // version they were set for: another version may declare different givens.
   // A change takes effect as it is made, and Reset puts every given back to
   // its declared default. A host that keeps them in its URL gets each applied
   // set back, so a link (and Copy link) carries what the page is running with.
   const declaredTypes = useMemo(() => declaredTypesOf(givens), [givens]);
   // Only the names this model declares: the host's other parameters (the
   // source on screen, say) changing must not read as new given values.
   const hostParamsKey = JSON.stringify(hostParams ?? null);
   const declaredParams = useMemo(
      () =>
         hostParams &&
         Object.fromEntries(
            Object.entries(hostParams).filter(([name]) =>
               declaredTypes.has(name),
            ),
         ),
      // eslint-disable-next-line react-hooks/exhaustive-deps -- `hostParams` by value
      [hostParamsKey, declaredTypes],
   );
   // Withheld until the model has loaded: before then nothing is declared, and
   // a report would tell the host to clear every parameter it wrote.
   const loaded = !isLoading && !isError;
   const reportGivens = useCallback(
      (next: Record<string, string>) =>
         onGivensChange?.(next, Array.from(declaredTypes.keys())),
      [onGivensChange, declaredTypes],
   );
   const givensState = useGivensState({
      declaredTypes,
      params: declaredParams,
      onParamsChange: loaded && onGivensChange ? reportGivens : undefined,
      documentKey: `${environmentName}/${packageName}@${versionId ?? ""}/${modelPath}`,
      autorun: true,
   });
   // Only the givens THIS model declares are sent: a value set against an
   // earlier version for a since-removed given must not reach the server.
   const givenValues = useMemo(
      () => givensRequest(givensState.applied, declaredTypes),
      [givensState.applied, declaredTypes],
   );
   // Each source's givens, as its Givens section shows them: what its
   // declaration reads (through what it extends or joins), plus the model-wide
   // ones no source reads. A run gates on the same set.
   const givensBySource = useMemo(
      () =>
         new Map(
            sources.map((s) => [
               s.name,
               givensForSource(sourceText, s.name, givens),
            ]),
         ),
      [sources, sourceText, givens],
   );
   const run = useMemo<RunContext>(
      () => ({
         projectName: environmentName,
         packageName,
         versionId,
         givens,
         givensBySource,
         requiredFiltersBySource,
         givenValues,
         accessBySource,
         includeHiddenFilesAndSources,
         queryMetadata,
         onQueryRun,
         maxResultSize,
      }),
      [
         environmentName,
         packageName,
         versionId,
         givens,
         givensBySource,
         requiredFiltersBySource,
         givenValues,
         accessBySource,
         includeHiddenFilesAndSources,
         queryMetadata,
         onQueryRun,
         maxResultSize,
      ],
   );

   // The source on screen: the host's when it keeps it, else this viewer's own.
   const [localSourceName, setLocalSourceName] = useState<string | undefined>(
      initialSourceName,
   );
   const controlled = controlledSourceName !== undefined;
   const requestedSourceName = controlled
      ? controlledSourceName
      : localSourceName;
   const pickSource = useCallback(
      (name: string) => {
         if (!controlled) setLocalSourceName(name);
         onSourceChange?.(name);
      },
      [controlled, onSourceChange],
   );
   const defaultSource = useMemo(
      () => pickDefaultSource(sources, modelPath),
      [sources, modelPath],
   );
   const requestedSource = sources.find((s) => s.name === requestedSourceName);
   const activeSource =
      requestedSource ?? (controlled ? undefined : defaultSource);

   const sourceGivens = useMemo(
      () =>
         activeSource
            ? (givensBySource.get(activeSource.name) ?? givens)
            : givens,
      [activeSource, givensBySource, givens],
   );
   // Which of those come from a source the page can't open, and from which.
   const inheritedGivens = useMemo(
      () =>
         activeSource
            ? givensInheritedBy(
                 sourceText,
                 activeSource.name,
                 givens,
                 sourceNames,
              )
            : new Map<string, string[]>(),
      [activeSource, sourceText, givens, sourceNames],
   );
   // The header's description: this source's own doc, less what it inherited
   // from the source it extends (named in the model text).
   const baseName = useMemo(
      () => baseSourceName(sourceText, activeSource?.name, sourceNames),
      [sourceText, activeSource?.name, sourceNames],
   );
   const activeSourceDoc = useMemo(
      () =>
         activeSource
            ? ownSourceDoc(
                 activeSource,
                 sources.find((s) => s.name === baseName),
              )
            : undefined,
      [activeSource, sources, baseName],
   );

   // A host's entities, by declaring source, and what they say about joins.
   const entitiesBySource = useMemo(
      () => (entities ? groupEntitiesBySource(entities) : NO_ENTITIES),
      [entities],
   );
   const entitiesByName = activeSource
      ? entitiesBySource.get(activeSource.name)
      : undefined;
   const { joinLinks, joinEntities, joinCodes, joinAccessRules } =
      useMemo(() => {
         const links = new Map<string, JoinLink>();
         const fields = new Map<string, EntitiesByField>();
         const codes = new Map<string, string>();
         const rules = new Map<
            string,
            { sourceName: string; access: SourceAccess }
         >();
         const browsable = new Map([[modelPath, new Set(sourceNames)]]);
         for (const entity of entitiesByName?.values() ?? []) {
            if (entity.fieldType !== "join") continue;
            if (entity.code) codes.set(entity.name, entity.code);
            const target = entity.joinTarget;
            if (!target) continue;
            if (target.modelPath === modelPath) {
               const joined = entitiesBySource.get(target.sourceName);
               if (joined) fields.set(entity.name, joined);
               const access = accessOf(accessBySource, target.sourceName);
               if (hasAccessRules(access))
                  rules.set(entity.name, {
                     sourceName: target.sourceName,
                     access,
                  });
               const reach = joinAccess(target, browsable);
               // Same model: an in-page selection, as the sources pill makes.
               links.set(
                  entity.name,
                  reach.kind === "link"
                     ? { ...reach, onOpen: () => pickSource(target.sourceName) }
                     : reach,
               );
            } else {
               const href = joinHref?.(target);
               if (href) links.set(entity.name, { kind: "link", href });
            }
         }
         return {
            joinLinks: links,
            joinEntities: fields,
            joinCodes: codes,
            joinAccessRules: rules,
         };
      }, [
         entitiesByName,
         entitiesBySource,
         modelPath,
         sourceNames,
         accessBySource,
         pickSource,
         joinHref,
      ]);

   // The picker, in the order a reader chooses from: on the package's root
   // `index.malloy`, the order it exports its sources in.
   const publicSurface =
      modelPath === "index.malloy" && !includeHiddenFilesAndSources;
   const pickerOrder = useMemo(
      () =>
         orderPickerSources(
            sources,
            defaultSource,
            publicSurface
               ? { exportOrder: exportOrder(sourceText) }
               : undefined,
         ),
      [sources, defaultSource, publicSurface, sourceText],
   );
   const describePickerSource = extensions?.describePickerSource;
   const navSources = useMemo<SourceNavItem[]>(
      () =>
         pickerOrder.sources.map((s) => {
            const extras = describePickerSource?.({
               modelPath,
               sourceName: s.name,
            });
            return {
               name: s.name,
               description: pickerDescription(s.doc, extras?.summary),
               descriptionPending:
                  !s.doc?.trim() &&
                  !extras?.summary &&
                  !!extras?.summaryPending,
               meta: sourceMetaLine(s),
               mostViews: pickerOrder.mostViews === s.name,
               access: accessOf(accessBySource, s.name),
               status: extras?.status,
            };
         }),
      [pickerOrder, accessBySource, describePickerSource, modelPath],
   );

   const body = (() => {
      if (isLoading) return <Loading text="Fetching Model..." />;
      if (isError) {
         // A refused read is an answer, not a failure: no Retry that asks the
         // same question again. In the server's words where it gave some.
         if (isNotFoundOrForbidden(error)) {
            return (
               <Box sx={{ pt: FILE_VIEW_TOP_GAP }}>
                  <EmptyRow
                     message={
                        serverMessage(error) ??
                        `${packageName}/${modelPath} doesn't exist, or you don't have access to it.`
                     }
                  />
               </Box>
            );
         }
         return (
            <LoadErrorAlert
               message={describeApiError("Failed to load model", error)}
               onRetry={() => void refetch()}
            />
         );
      }
      const queriesGroup = queries.length > 0 && (
         <QueriesGroup
            queries={queries}
            sourceText={sourceText}
            modelPath={modelPath}
            run={run}
         />
      );
      if (sources.length === 0) {
         if (queries.length === 0) {
            // Still the model's page, with its heading and actions (Copy
            // link): a givens-only file is a model like any other.
            return (
               <>
                  <HeaderBar
                     headingLevel={headingLevel}
                     title={modelDisplayName(modelPath)}
                     actions={headerActions}
                     isSource={false}
                     offSurface={includeHiddenFilesAndSources}
                  />
                  <EmptyRow
                     message="This model defines no sources or queries."
                     secondary={
                        hasImports && (
                           <>
                              It imports other files but re-exports nothing. Add{" "}
                              <code>export {"{ source_name }"}</code> to surface
                              imported sources here.
                           </>
                        )
                     }
                  />
               </>
            );
         }
         // No sources, but named queries: the page is its queries, titled by
         // the model (there is no source to name it).
         return (
            <>
               <HeaderBar
                  headingLevel={headingLevel}
                  title={modelDisplayName(modelPath)}
                  meta={`${queries.length} ${queries.length === 1 ? "query" : "queries"}`}
                  actions={headerActions}
                  isSource={false}
                  offSurface={includeHiddenFilesAndSources}
               />
               {queriesGroup}
            </>
         );
      }
      if (!activeSource) {
         // A name the model doesn't expose (renamed, removed, or hidden by the
         // package's surface): a clean not-found state, never another source
         // under the wrong name, and alike for "never existed" and "hidden".
         return (
            <Box sx={{ pt: FILE_VIEW_TOP_GAP }}>
               <EmptyRow
                  message={`This model has no source named ${requestedSourceName}.`}
                  secondary={
                     <Box
                        component="span"
                        data-testid="model-viewer-source-links"
                     >
                        {"Sources in this model: "}
                        {sources.map((s, i) => (
                           <span key={s.name}>
                              {i > 0 && ", "}
                              <Link
                                 component="button"
                                 type="button"
                                 onClick={() => pickSource(s.name)}
                                 sx={{
                                    fontFamily: MONO_FONT_FAMILY,
                                    verticalAlign: "baseline",
                                 }}
                              >
                                 {s.name}
                              </Link>
                           </span>
                        ))}
                     </Box>
                  }
               />
            </Box>
         );
      }
      const n = fieldCount(activeSource);
      // A derived source names the source it's built from, which picks it in
      // this same viewer (an in-page selection, like the sources pill); one on
      // a table names the table, where a host's entities say which.
      const table = baseName
         ? undefined
         : [...(entitiesByName?.values() ?? [])].find((e) => e.table)?.table;
      const provenance = baseName ? (
         <>
            {"from "}
            <Link
               component="button"
               type="button"
               onClick={() => pickSource(baseName)}
               sx={{
                  fontFamily: MONO_FONT_FAMILY,
                  color: TEXT_TERTIARY,
                  textDecorationColor: "inherit",
                  verticalAlign: "baseline",
               }}
            >
               {baseName}
            </Link>
         </>
      ) : table ? (
         <>
            {"reads "}
            <Box
               component="span"
               data-testid="model-viewer-source-table"
               sx={{ fontFamily: MONO_FONT_FAMILY }}
            >
               {[
                  table.connectionName,
                  table.catalogName,
                  table.schemaName,
                  table.tableName,
               ]
                  .filter(Boolean)
                  .join(".")}
            </Box>
         </>
      ) : undefined;
      const sourceDetail = extensions?.renderSourceDetail?.({
         modelPath,
         sourceName: activeSource.name,
      });
      return (
         <>
            <HeaderBar
               headingLevel={headingLevel}
               title={activeSource.name}
               titleAdornments={
                  sources.length > 1 && (
                     <SourcesNav
                        sources={navSources}
                        currentName={activeSource.name}
                        countLabel={sourceCountLabel(
                           navSources.length,
                           pickerOrder.curated,
                        )}
                        onPickSource={pickSource}
                        curated={pickerOrder.curated}
                        exportOrdered={pickerOrder.exportOrdered}
                     />
                  )
               }
               actions={headerActions}
               status={renderSourceStatus?.(activeSource.name)}
               kindLabel="Source"
               meta={`${n} ${n === 1 ? "field" : "fields"}`}
               doc={activeSourceDoc}
               modelDoc={modelDoc}
               access={accessOf(accessBySource, activeSource.name)}
               showRules={showAccessRules}
               offSurface={includeHiddenFilesAndSources}
               publicSurface={publicSurface}
               provenance={provenance}
            />
            {sourceDetail && (
               <Box data-testid="model-viewer-source-detail" sx={{ mt: 1.5 }}>
                  {sourceDetail}
               </Box>
            )}
            <ModelGivensContext.Provider value={givens}>
               <SchemaView
                  // Keyed by source: switching source presents it fresh, with
                  // none of the previous source's rows left open.
                  key={activeSource.name}
                  source={activeSource}
                  modelPath={modelPath}
                  entitiesByName={entitiesByName}
                  entitiesLoading={entitiesLoading}
                  entitiesError={entitiesError}
                  columnDescriptions={columnDescriptions?.get(
                     activeSource.name,
                  )}
                  modelText={sourceText}
                  joinLinks={joinLinks}
                  joinAccess={joinAccessRules}
                  showRules={showAccessRules}
                  joinEntities={joinEntities}
                  joinCodes={joinCodes}
                  run={run}
                  queries={queriesGroup}
                  givens={
                     <ModelGivensSection
                        givens={sourceGivens}
                        inheritedFrom={inheritedGivens}
                        values={givensState.draft}
                        onChange={givensState.setGiven}
                        onReset={givensState.reset}
                        environmentName={environmentName}
                        packageName={packageName}
                        modelPath={modelPath}
                        versionId={versionId}
                        declaredTypes={declaredTypes}
                     />
                  }
               />
            </ModelGivensContext.Provider>
         </>
      );
   })();

   return (
      <ModelViewerExtensionsContext.Provider value={extensions ?? {}}>
         <Box
            data-testid="model-viewer"
            // The host's code face, for every name and block of Malloy below.
            sx={{ ...CONTENT_COLUMN_SX, pb: 2, [MONO_FONT_VAR]: monoFont }}
         >
            <Box sx={FLAT_PANE_SX}>{body}</Box>
         </Box>
      </ModelViewerExtensionsContext.Provider>
   );
}
