import {
   encodeResourceUri,
   Notebook,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import { useCallback, useMemo, useRef } from "react";
import { useSearchParams } from "react-router-dom";

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
   const [searchParams, setSearchParams] = useSearchParams();
   const navigate = useRouterClickHandler();

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
         writtenRef.current = new Set([
            ...writtenRef.current,
            ...Object.keys(next),
         ]);
         setSearchParams(
            (current) => {
               // Merged into what is already there, not written over it. The
               // page's query string is not ours alone: a tracking tag or any
               // other unrelated parameter shares it, and replacing the whole
               // string deletes it on the first control change. The SDK takes
               // the same care in the other direction: `paramsToGivens`
               // ignores names the model does not declare, and a wholesale
               // replace here would undo that.
               const merged = new URLSearchParams(current);
               // Scoped to the names that are ours: what the notebook manages
               // now, plus anything this page wrote earlier. A cleared control
               // has to leave the address bar, and so does a given the model no
               // longer declares. Anything outside that set is left exactly as
               // found, which is the whole point of merging.
               for (const name of ours) {
                  if (!(name in next)) merged.delete(name);
               }
               for (const [name, value] of Object.entries(next)) {
                  merged.set(name, value);
               }
               return merged;
            },
            // Changing a parameter is not a navigation step: Back should leave
            // the notebook, not walk back through every value the reader tried.
            { replace: true },
         );
      },
      [setSearchParams],
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
         />
      </Box>
   );
}
