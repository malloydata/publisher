// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Alert,
   Box,
   Button,
   ButtonBase,
   Stack,
   TextField,
   Tooltip,
   Typography,
} from "@mui/material";
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type { GivenValue } from "../../hooks/givenValue";
import { reportClasses } from "../../theme/classes";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { edgeColor } from "../controlTokens";
import { DashboardTile } from "../Dashboard/DashboardTile";
import {
   readableName,
   ViewPicker,
   type PickerGroup,
} from "../ReportBuilder/ViewPicker";
import {
   createDocument,
   reportPathForTitle,
   MAX_SLUG_ATTEMPTS,
   newDocumentProblem,
   reportPathProblem,
   useDocumentChoices,
   type CreatedDocument,
   type CreateTarget,
} from "../DocumentCreate";
import type { ReportEventHandler } from "../Report/telemetry";
import { canRetryRequest } from "../DocumentCreate/canRetry";
import { AppDialog } from "../AppDialog";
import { layoutOfKind, type DocumentKind } from "../legacy/kindAdapter";
import type { ReportLayout } from "./documentLocation";

export interface NewDocumentDialogProps {
   open: boolean;
   /** Which layout the dialog opens on. Default linear. */
   layout?: ReportLayout;
   /** @deprecated Pass `layout`: `dashboard` is grid, `notebook` linear. */
   kind?: DocumentKind;
   /** Whether the reader may switch the layout. Default true. */
   allowLayoutChange?: boolean;
   /** @deprecated Pass `allowLayoutChange`. */
   allowKindChange?: boolean;
   environmentName: string;
   packageName: string;
   versionId?: string;
   /** The package's model files, relative to its root, to pick a source from. */
   models: readonly string[];
   /** The host is still listing `models`, so an empty list is not yet "no models". */
   modelsLoading?: boolean;
   /** The host's listing of `models` failed, so an empty list is not "no models". */
   modelsError?: unknown;
   /** Lists the models again; offered when `modelsError` is one a retry can clear. */
   onRetryModels?: () => void;
   /** Where the file is written, and (on the package route) which names are taken. */
   target: CreateTarget;
   /** The line under the title on the storage route, saying where the host keeps the new file. */
   savedAs?: string;
   onClose: () => void;
   onCreated: (created: CreatedDocument) => void;
   /** `report.created` or `report.create_refused`; see `ReportEvent`. */
   onEvent?: ReportEventHandler;
}

/**
 * A new report: pick the model, the view its first query tile shows, a title
 * and a layout. The file is written as `<slug>.malloy` at the package root, or
 * at the location the author types instead. It is the file the builder would
 * write, so everything after this is the builder's; the host opens it from
 * `onCreated`.
 */
export function NewDocumentDialog({
   open,
   layout: layoutProp,
   kind: kindProp,
   allowLayoutChange,
   allowKindChange,
   environmentName,
   packageName,
   versionId,
   models,
   modelsLoading = false,
   modelsError,
   onRetryModels,
   target,
   savedAs,
   onClose,
   onCreated,
   onEvent,
}: NewDocumentDialogProps) {
   const initialLayout: ReportLayout =
      layoutProp ?? (kindProp ? layoutOfKind(kindProp) : "linear");
   const canChangeLayout = allowLayoutChange ?? allowKindChange ?? true;
   const [layout, setLayout] = useState<ReportLayout>(initialLayout);
   /** The location as typed; undefined until the author edits it, and the title then names the file. */
   const [location, setLocation] = useState<string | undefined>(undefined);
   const [modelPath, setModelPath] = useState("");
   /** The chosen view, as `source::view` — one value, so the pair is always valid. */
   const [tile, setTile] = useState("");
   const [query, setQuery] = useState("");
   const searchRef = useRef<HTMLInputElement>(null);
   const [title, setTitle] = useState("");
   const [touchedTitle, setTouchedTitle] = useState(false);
   const [attempted, setAttempted] = useState(false);
   const [busy, setBusy] = useState(false);
   const [failure, setFailure] = useState<string | undefined>(undefined);
   const modelsFailed = modelsError !== undefined && modelsError !== null;

   // Every model at once rather than the chosen one: a model that declares no
   // source with a view cannot start a document, and the only way to offer a
   // list without those is to know before the reader picks.
   const { choices, isLoading, isSuccess, failed, retry, canRetry } =
      useDocumentChoices({
         environmentName,
         packageName,
         ...(versionId !== undefined ? { versionId } : {}),
         models,
         enabled: open && models.length > 0,
      });

   const tiles = useMemo(
      () => choices.get(modelPath) ?? [],
      [choices, modelPath],
   );
   // Every model's views in one list, grouped by source; a key names the
   // model too, so picking a view picks its model.
   // What each picker key names, so a key is looked up, never split: a model
   // path or a name could hold the separator.
   const picks = useMemo(
      () =>
         new Map(
            [...choices.entries()].flatMap(([model, pairs]) =>
               pairs.map(
                  (pair) =>
                     [
                        pickKey(model, pair.source, pair.view),
                        { model, source: pair.source, view: pair.view },
                     ] as const,
               ),
            ),
         ),
      [choices],
   );
   const groups = useMemo<PickerGroup[]>(() => {
      const several = choices.size > 1;
      return [...choices.entries()].flatMap(([model, pairs]) => {
         const bySource = new Map<string, typeof pairs>();
         for (const pair of pairs)
            bySource.set(pair.source, [
               ...(bySource.get(pair.source) ?? []),
               pair,
            ]);
         return [...bySource.entries()].map(([source, views]) => ({
            key: `${model}::${source}`,
            label: source,
            ariaLabel: `${source} in ${model}`,
            ...(several || views[0]?.sourceDescription
               ? {
                    detail: [
                       several ? model : undefined,
                       views[0]?.sourceDescription,
                    ]
                       .filter(Boolean)
                       .join(" · "),
                 }
               : {}),
            entries: views.map((pair) => ({
               key: pickKey(model, pair.source, pair.view),
               name: pair.view,
               title: pair.description ?? readableName(pair.view),
               ...(pair.chart ? { chart: pair.chart } : {}),
            })),
         }));
      });
   }, [choices]);
   const pickView = (key: string) => {
      const pick = picks.get(key);
      if (!pick) return;
      setModelPath(pick.model);
      setTile(`${pick.source}::${pick.view}`);
   };
   const chosen = tiles.find((t) => `${t.source}::${t.view}` === tile);
   const pickedKey = chosen
      ? pickKey(modelPath, chosen.source, chosen.view)
      : undefined;
   // The preview follows the pick once it settles: arrowing through the list
   // ran a query for every view passed on the way.
   const previewed = useSettled(chosen, 250);

   useEffect(() => {
      if (open) setLayout(initialLayout);
   }, [open, initialLayout]);

   // Everything with one answer answers itself: a package with one usable
   // model, a model with one view, and a title taken from that view.
   useEffect(() => {
      if (!open) return;
      const usable = [...choices.keys()];
      if (modelPath === "" && usable.length > 0) setModelPath(usable[0]);
   }, [open, choices, modelPath]);

   // Only a pick not yet made is filled in: a picked view that leaves the list stays unpicked, never swapped.
   useEffect(() => {
      if (!open || tiles.length === 0 || tile !== "") return;
      setTile(`${tiles[0].source}::${tiles[0].view}`);
   }, [open, tiles, tile]);

   useEffect(() => {
      if (!open || touchedTitle || chosen === undefined) return;
      setTitle(readableName(chosen.view));
   }, [open, touchedTitle, chosen]);

   useEffect(() => {
      if (open) return;
      // Reset on close, so the next open starts from the package again.
      setModelPath("");
      setTile("");
      setQuery("");
      setTitle("");
      setTouchedTitle(false);
      setLocation(undefined);
      setAttempted(false);
      setFailure(undefined);
   }, [open]);

   // The first name at the package root the listing leaves free; the storage
   // route checks each name against the store itself when it writes.
   const suggested = useMemo(() => {
      if (title.trim() === "") return undefined;
      for (let n = 1; n <= MAX_SLUG_ATTEMPTS; n++) {
         const candidate = reportPathForTitle(title, n);
         if (!target.existing?.includes(candidate)) return candidate;
      }
      return undefined;
   }, [title, target]);
   const typed = location?.trim();
   // A typed location is taken as written, `.malloy` added when it is left off.
   const typedPath =
      typed === undefined || typed === ""
         ? undefined
         : typed.endsWith(".malloy")
           ? typed
           : `${typed}.malloy`;
   const path = typedPath ?? suggested;
   const locationProblem =
      typedPath === undefined
         ? undefined
         : (reportPathProblem(typedPath) ??
           (target.existing?.includes(typedPath)
              ? `${typedPath} already exists; choose another location.`
              : undefined));
   const problem =
      chosen === undefined
         ? undefined
         : newDocumentProblem({
              title,
              modelPath,
              source: chosen.source,
              view: chosen.view,
           });
   const titleProblem =
      problem ??
      (title.trim() !== "" && path === undefined
         ? "No free file name for this title; choose another."
         : undefined);
   // A blank title is said only once Create is pressed; anything else as it is typed.
   const showTitleProblem =
      titleProblem !== undefined && (attempted || title.trim() !== "");

   const loading = modelsLoading || isLoading;
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const edge = edgeColor(theme.cardBorder);
   const unread = choices.size === 0 && (modelsFailed || failed.length > 0);
   // Create is off only when nothing typed could make it succeed; the title is checked on press.
   const blocked = loading
      ? "Loading model views"
      : unread
        ? "Couldn't read the model views to start from"
        : choices.size === 0
          ? "No model view to start from yet"
          : chosen === undefined
            ? "Pick a view to start from"
            : undefined;
   const label = "report";
   const createLabel = busy ? `Creating ${label}…` : `Create ${label}`;
   const picked = tile !== "" && chosen === undefined && tiles.length > 0;

   const create = async () => {
      if (blocked !== undefined || chosen === undefined || busy) return;
      if (titleProblem !== undefined || locationProblem !== undefined) {
         setAttempted(true);
         return;
      }
      setBusy(true);
      setFailure(undefined);
      try {
         const created = await createDocument({
            layout,
            ...(typedPath !== undefined ? { path: typedPath } : {}),
            document: {
               title,
               modelPath,
               source: chosen.source,
               view: chosen.view,
            },
            target,
            ...(onEvent ? { onEvent } : {}),
         });
         onCreated(created);
      } catch (error) {
         const message = (
            error as { response?: { data?: { message?: string } } }
         ).response?.data?.message;
         setFailure(
            message ?? (error instanceof Error ? error.message : String(error)),
         );
      } finally {
         setBusy(false);
      }
   };

   const retryButton = (onRetry: () => void) => (
      <Button color="inherit" size="small" onClick={onRetry}>
         Retry
      </Button>
   );

   return (
      <AppDialog
         className={`${reportClasses.dialog} ${reportClasses.newReportDialog}`}
         maxWidth="md"
         initialFocus={searchRef}
         open={open}
         // A write in flight is not abandoned by Escape or a backdrop click.
         onClose={busy ? () => {} : onClose}
         title={`New ${label}`}
         description="A report is a Malloy file in the package that lays out text and queries. Pick the view its first query shows; the rest is the builder."
         actions={
            <>
               <Button onClick={onClose} disabled={busy}>
                  Cancel
               </Button>
               {/* A disabled button fires no events, so the tooltip hangs on a wrapper. */}
               <Tooltip title={busy ? "" : (blocked ?? "")}>
                  <span>
                     <Button
                        variant="contained"
                        disabled={busy || blocked !== undefined}
                        aria-label={
                           !busy && blocked !== undefined
                              ? `${createLabel}: ${blocked}`
                              : createLabel
                        }
                        onClick={() => void create()}
                     >
                        {createLabel}
                     </Button>
                  </span>
               </Tooltip>
            </>
         }
      >
         <Stack sx={{ gap: 1.5 }}>
            {modelsFailed ? (
               <Alert
                  severity="error"
                  action={
                     onRetryModels && canRetryRequest(modelsError)
                        ? retryButton(onRetryModels)
                        : undefined
                  }
               >
                  Couldn&apos;t read this package&apos;s models, so no view is
                  listed to start from.
               </Alert>
            ) : models.length === 0 ? (
               !modelsLoading && (
                  <Alert severity="info">
                     This package has no models yet. Add a .malloy file that
                     declares a source with a view, then create a {label} from
                     it.
                  </Alert>
               )
            ) : failed.length > 0 ? (
               <Alert
                  severity="warning"
                  action={canRetry ? retryButton(retry) : undefined}
               >
                  Couldn&apos;t read {failed.join(", ")}, so{" "}
                  {failed.length === 1 ? "its" : "their"} views aren&apos;t
                  listed.
               </Alert>
            ) : (
               isSuccess &&
               choices.size === 0 && (
                  <Alert severity="info">
                     A {label} starts from a view of a source, and no model in
                     this package declares one yet. Add a view to a source in a
                     model file and this list fills in.
                  </Alert>
               )
            )}
            <Box
               sx={{
                  // Edge to edge inside the window: two panes, as Add a tile.
                  mx: -3,
                  mb: -1,
                  borderTop: `1px solid ${edge}`,
                  borderBottom: `1px solid ${edge}`,
                  display: "grid",
                  gridTemplateColumns: {
                     xs: "1fr",
                     md: "320px minmax(0, 1fr)",
                  },
                  height: { md: 500 },
               }}
            >
               <Box
                  sx={{
                     display: "flex",
                     flexDirection: "column",
                     minHeight: 0,
                     borderRight: { md: `1px solid ${edge}` },
                     maxHeight: { xs: 280, md: "none" },
                  }}
               >
                  <ViewPicker
                     groups={groups}
                     query={query}
                     onQuery={setQuery}
                     picked={pickedKey}
                     onPick={pickView}
                     onTake={(key) => {
                        pickView(key);
                     }}
                     searchRef={searchRef}
                     disabled={busy}
                     empty={
                        <Typography
                           variant="body2"
                           sx={{ color: "text.secondary", p: 2 }}
                        >
                           {loading
                              ? "Reading the package's models…"
                              : "No view to start from."}
                        </Typography>
                     }
                  />
               </Box>
               <Box
                  sx={(mui) => ({
                     p: 2.5,
                     display: "flex",
                     flexDirection: "column",
                     gap: 2,
                     minHeight: 0,
                     overflowY: "auto",
                     bgcolor: `color-mix(in srgb, ${mui.palette.text.primary} 3%, ${mui.palette.background.paper})`,
                  })}
               >
                  {canChangeLayout && (
                     <Box>
                        <PaneLabel>Layout</PaneLabel>
                        <Box
                           role="radiogroup"
                           aria-label="Layout"
                           sx={{
                              display: "grid",
                              gridTemplateColumns: "1fr 1fr",
                              gap: 1,
                           }}
                        >
                           {(["linear", "grid"] as const).map((l) => (
                              <LayoutCard
                                 key={l}
                                 layout={l}
                                 checked={layout === l}
                                 disabled={busy}
                                 onPick={() => setLayout(l)}
                              />
                           ))}
                        </Box>
                        <Typography
                           variant="caption"
                           component="p"
                           sx={{ color: "text.secondary", mt: 0.75 }}
                        >
                           Switch it any time in the builder.
                        </Typography>
                     </Box>
                  )}
                  <Box>
                     <PaneLabel>First query</PaneLabel>
                     <Box
                        role="region"
                        aria-label="Preview"
                        sx={(mui) => ({
                           height: 200,
                           display: "flex",
                           position: "relative",
                           overflow: "hidden",
                           borderRadius: ui.radius.tile,
                           border: `1px solid ${edge}`,
                           bgcolor: mui.palette.background.paper,
                           "&::after": {
                              content: '""',
                              position: "absolute",
                              left: 0,
                              right: 0,
                              bottom: 0,
                              height: 40,
                              pointerEvents: "none",
                              background: `linear-gradient(to bottom, transparent, ${mui.palette.background.paper})`,
                           },
                        })}
                     >
                        {chosen && previewed ? (
                           <Box sx={{ flex: 1, minWidth: 0, px: 2, pt: 1.5 }}>
                              <DashboardTile
                                 environmentName={environmentName}
                                 packageName={packageName}
                                 {...(versionId !== undefined
                                    ? { versionId }
                                    : {})}
                                 modelPath={modelPath}
                                 tile={`${previewed.source} -> ${previewed.view}`}
                                 label={
                                    title.trim() || readableName(previewed.view)
                                 }
                                 borderless
                                 givens={NO_GIVENS}
                                 declaredTypes={NO_TYPES}
                                 height={180}
                              />
                           </Box>
                        ) : (
                           <Typography
                              variant="body2"
                              sx={{ color: "text.secondary", m: "auto" }}
                           >
                              {picked
                                 ? "The view you picked is no longer in this model; pick another."
                                 : "Pick a view to start from."}
                           </Typography>
                        )}
                     </Box>
                     <Typography
                        variant="caption"
                        component="p"
                        sx={{ color: "text.secondary", mt: 0.75 }}
                     >
                        More are added in the builder.
                     </Typography>
                  </Box>
                  <TextField
                     size="small"
                     label="Title"
                     value={title}
                     disabled={busy}
                     onChange={(event) => {
                        setTouchedTitle(true);
                        setTitle(event.target.value);
                     }}
                     InputLabelProps={{ shrink: true }}
                     inputProps={{ "aria-label": "Report title" }}
                     error={showTitleProblem}
                     helperText={
                        showTitleProblem
                           ? titleProblem
                           : target.route === "package"
                             ? "Names the file too, unless you type a location."
                             : (savedAs ??
                               `Saved as a new ${label} in the host's store, and opened in the builder.`)
                     }
                  />
                  <TextField
                     size="small"
                     label="Location"
                     value={location ?? suggested ?? ""}
                     placeholder="report.malloy"
                     disabled={busy}
                     onChange={(event) => setLocation(event.target.value)}
                     InputLabelProps={{ shrink: true }}
                     inputProps={{ "aria-label": "Location" }}
                     error={locationProblem !== undefined}
                     helperText={
                        locationProblem ??
                        (path
                           ? `Written as ${path} in the package, and opened in the builder.`
                           : "A path in the package; the package root by default.")
                     }
                  />
                  {failure && <Alert severity="error">{failure}</Alert>}
               </Box>
            </Box>
         </Stack>
      </AppDialog>
   );
}

/** `value` once it has held still for `ms`; undefined until then. */
function useSettled<T>(value: T, ms: number): T | undefined {
   const [settled, setSettled] = useState<T | undefined>(undefined);
   useEffect(() => {
      const timer = window.setTimeout(() => setSettled(value), ms);
      return () => window.clearTimeout(timer);
   }, [value, ms]);
   return settled;
}
/** A picker key: the model, the source and the view, so the pick names all three. */
const pickKey = (model: string, source: string, view: string) =>
   `${model}::${source}::${view}`;

/** A preview runs with no filter values: the model's own defaults. */
const NO_GIVENS = new Map<string, GivenValue>();
const NO_TYPES = new Map<string, string | undefined>();

/** A small heading over a part of the window. */
function PaneLabel({ children }: { children: ReactNode }) {
   const ui = usePublisherTheme().theme.ui;
   return (
      <Typography
         component="div"
         sx={{ ...ui.type.label, color: "text.secondary", mb: 0.75 }}
      >
         {children}
      </Typography>
   );
}

/** A layout as a card: its picture and its name, as the filter kinds are. */
function LayoutCard({
   layout,
   checked,
   disabled,
   onPick,
}: {
   layout: ReportLayout;
   checked: boolean;
   disabled: boolean;
   onPick: () => void;
}) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const edge = edgeColor(theme.cardBorder);
   const ink = checked ? theme.accent : "currentColor";
   return (
      <ButtonBase
         role="radio"
         aria-checked={checked}
         aria-label={layout === "linear" ? "Linear" : "Grid"}
         disabled={disabled}
         onClick={onPick}
         sx={(mui) => ({
            justifyContent: "flex-start",
            gap: 1.25,
            px: 1.25,
            py: 1,
            borderRadius: ui.radius.tile,
            border: `1px solid ${checked ? theme.accent : edge}`,
            boxShadow: checked ? `0 0 0 1px ${theme.accent}` : "none",
            bgcolor: checked
               ? `color-mix(in srgb, ${theme.accent} 6%, ${mui.palette.background.paper})`
               : mui.palette.background.paper,
            color: mui.palette.text.secondary,
            textAlign: "left",
         })}
      >
         <svg width="34" height="26" viewBox="0 0 34 26" aria-hidden>
            {layout === "linear" ? (
               <g fill="none" stroke={ink} strokeWidth="1.5">
                  <rect x="3" y="2" width="28" height="7" rx="1.5" />
                  <rect x="3" y="12" width="28" height="5" rx="1.5" />
                  <path d="M5 21.5h20" strokeLinecap="round" />
               </g>
            ) : (
               <g fill="none" stroke={ink} strokeWidth="1.5">
                  <rect x="2" y="2" width="14" height="10" rx="1.5" />
                  <rect x="18" y="2" width="14" height="10" rx="1.5" />
                  <rect x="2" y="14" width="30" height="10" rx="1.5" />
               </g>
            )}
         </svg>
         <Box>
            <Box
               sx={{
                  ...ui.type.control,
                  fontWeight: checked ? ui.type.label.fontWeight : 500,
                  color: checked ? theme.accent : "text.primary",
               }}
            >
               {layout === "linear" ? "Linear" : "Grid"}
            </Box>
            <Box sx={{ ...ui.type.caption, color: "text.secondary" }}>
               {layout === "linear"
                  ? "Read top to bottom"
                  : "Tiles side by side"}
            </Box>
         </Box>
      </ButtonBase>
   );
}
