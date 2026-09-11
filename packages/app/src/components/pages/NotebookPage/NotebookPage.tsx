// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   Notebook,
   useRouterClickHandler,
   type DrillNavigation,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import { useCallback, useMemo, useRef } from "react";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";

export interface NotebookPageProps {
   environmentName: string;
   packageName: string;
   /** Package-relative path, e.g. `storefront.malloynb`. */
   notebookPath: string;
}

/**
 * The Console's host for the SDK `Notebook`.
 *
 * The component reads nothing from the router; the URL sync that makes a
 * parameterized notebook a shareable link lands here.
 */
export default function NotebookPage({
   environmentName,
   packageName,
   notebookPath,
}: NotebookPageProps) {
   const [searchParams] = useSearchParams();
   const navigate = useRouterClickHandler();

   // `setSearchParams` is deliberately not used to WRITE. It navigates to a
   // search-only target (`"?" + params`), and a target carrying no fragment
   // resolves to an empty one, so changing a control dropped whatever anchor
   // the reader had arrived on. Navigating with an explicit `hash` keeps it.
   const routerNavigate = useNavigate();
   const location = useLocation();
   // `location` is read from the closure, NOT through a ref. Two earlier shapes
   // were both wrong: assigning the ref during render keeps a location from a
   // render React may discard, and assigning it in an effect reads stale,
   // because the report that calls this arrives from an effect inside
   // `Notebook`, a CHILD, and React flushes child effects before the parent's.
   // On an in-app notebook-to-notebook navigation with a warm cache the report
   // fires on the very commit that changes the notebook, so the merge ran
   // against the notebook the reader had just left: it carried that page's
   // fragment and its unrelated parameters onto the new one.
   //
   // Capturing the value is correct because the callback handed down on a given
   // render belongs to that render's location. It costs a new callback identity
   // per URL change, which is harmless: the effect that calls it compares the
   // values first and does nothing when they match.

   const givens = useMemo(
      () => Object.fromEntries(searchParams.entries()),
      [searchParams],
   );

   // Names this page has written at least once. `managed` is what the notebook
   // declares *now*, and a package reload can drop a given: its value is pruned
   // from the state but its parameter would stay in the URL forever, riding along
   // in any copied link and silently re-applying if the given is ever
   // re-declared. Anything we put there is ours to take back out.
   //
   // With one hole, documented in `useGivensState`: when the manifest is
   // already cached the hook fires no INITIAL report, so a name that was only
   // ever carried in by the URL never reaches this set. If a later reload drops
   // that given, its parameter is left behind. Inert, because `paramsToGivens`
   // ignores a name the model does not declare, and the alternative wipes a
   // shared link's parameters on mount.
   //
   // Deliberately NOT reset when `notebookPath` changes: a parameter this page
   // wrote for the previous notebook is the stale one the next should clear, and
   // resetting here would reintroduce the orphan across a navigation instead of
   // only across a reload. Bounded by the number of distinct given names visited.
   //
   // That is only safe because `Notebook` stays silent until it has loaded. Were
   // it to report "no values, nothing managed" while the next notebook was
   // still fetching, this loop would delete parameters that belonged to the
   // notebook arriving: worst when both notebooks import the same source and so
   // declare the same given. The guard lives there rather than here because only
   // the component knows the difference between "no givens" and "not yet known".
   const writtenRef = useRef<Set<string>>(new Set());

   const onGivensChange = useCallback(
      (next: Record<string, string>, managed: readonly string[]) => {
         const ours = new Set([...managed, ...writtenRef.current]);
         // `managed` as well as the names carried in `next`: a given the notebook
         // declares is this page's to clean up even when it currently has no
         // value, which is how a control cleared before a model reload still
         // gets its parameter removed.
         writtenRef.current = new Set([
            ...writtenRef.current,
            ...Object.keys(next),
            ...managed,
         ]);
         // Merged into what is already there, not written over it. The page's
         // query string is not ours alone: a tracking tag or any other
         // unrelated parameter shares it, and replacing the whole string
         // deletes it on the first control change. The SDK takes the same care
         // in the other direction: `paramsToGivens` ignores names the model
         // does not declare, and a wholesale replace here would undo that.
         const merged = new URLSearchParams(location.search);
         // Scoped to the names that are ours: what the notebook manages now,
         // plus anything this page wrote earlier. A cleared control has to
         // leave the address bar, and so does a given the model no longer
         // declares. Anything outside that set is left exactly as found, which
         // is the whole point of merging.
         for (const name of ours) {
            // `hasOwnProperty`, not `in`: `next` is a plain object literal, so
            // `in` walks its prototype and reports `constructor`, `toString`
            // and friends as present. A given with one of those names could
            // never satisfy this branch, so clearing its control left the
            // parameter in the address bar for good, silently re-applying to
            // anyone who opened the link.
            if (!Object.prototype.hasOwnProperty.call(next, name)) {
               merged.delete(name);
            }
         }
         for (const [name, value] of Object.entries(next)) {
            merged.set(name, value);
         }
         routerNavigate(
            { search: merged.toString(), hash: location.hash },
            // Changing a parameter is not a navigation step: Back should leave
            // the notebook, not walk back through every value the reader tried.
            { replace: true },
         );
      },
      [routerNavigate, location],
   );

   // A `# drill` naming a dashboard leaves the notebook, so unlike a filter
   // change it pushes history: Back returns to the notebook the reader drilled
   // from. `navigate` also honours cmd/ctrl-click by opening a new tab. Every
   // segment is encoded, since the destination slug comes from a tag naming a
   // filename and can hold characters that would read as structure in a path.
   const onDrillNavigate = useCallback(
      (target: DrillNavigation, event?: MouseEvent) => {
         const query = new URLSearchParams(target.givens).toString();
         const env = encodeURIComponent(environmentName);
         const pkg = encodeURIComponent(packageName);
         const slug = encodeURIComponent(target.dashboard);
         navigate(
            `/${env}/${pkg}/dashboards/${slug}` + (query ? `?${query}` : ""),
            event,
         );
      },
      [navigate, environmentName, packageName],
   );

   return (
      <Box sx={{ p: 3, maxWidth: 1200, mx: "auto" }}>
         <Notebook
            resourceUri={encodeResourceUri({
               environmentName,
               packageName,
               modelPath: notebookPath,
            })}
            maxResultSize={1024 * 1024}
            givens={givens}
            onGivensChange={onGivensChange}
            onNavigate={navigate}
            onDrillNavigate={onDrillNavigate}
         />
      </Box>
   );
}
