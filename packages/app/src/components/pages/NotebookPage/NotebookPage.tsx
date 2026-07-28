import {
   encodeResourceUri,
   Notebook,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import { useCallback, useMemo } from "react";
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
 * The counterpart of `DashboardPage`, and deliberately the same shape: the
 * component reads nothing from the router, and the URL sync that makes a
 * parameterized notebook a shareable link lands here. Two surfaces, one
 * contract.
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

   const onGivensChange = useCallback(
      (next: Record<string, string>) => {
         // Changing a parameter is not a navigation step: Back should leave the
         // notebook, not walk back through every value the reader tried.
         setSearchParams(next, { replace: true });
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
