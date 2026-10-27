// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import {
   Box,
   IconButton,
   Snackbar,
   Stack,
   Tooltip,
   Typography,
} from "@mui/material";
import React, { useState } from "react";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { ModelCell } from "./ModelCell";
import { ModelSources } from "./ModelSources";
import { useModelData } from "./useModelData";

export interface ModelProps {
   resourceUri: string;
   runOnDemand?: boolean;
   maxResultSize?: number;
}

/** The compiled model's sources, which arrive as JSON text; one that does not parse is left out. */
function parseSourceInfos(
   sourceInfos: string[] | undefined,
): Malloy.SourceInfo[] {
   const parsed: Malloy.SourceInfo[] = [];
   for (const text of sourceInfos ?? []) {
      try {
         parsed.push(JSON.parse(text) as Malloy.SourceInfo);
      } catch {
         continue;
      }
   }
   return parsed;
}

/**
 * A model's page: the sources it exposes with their fields, and its named
 * queries, each of which runs.
 *
 * There is no query builder here. The Explorer this page used to embed wrote
 * structured queries back to Malloy text through `@malloydata/malloy-interfaces`,
 * whose string escaping is a TODO, so a value holding a double quote rewrote
 * the query it was drilled into.
 */
export default function Model({
   resourceUri,
   runOnDemand = false,
   maxResultSize = 0,
}: ModelProps) {
   const { modelPath } = parseResourceUri(resourceUri);
   const { data, isError, isLoading, error } = useModelData(resourceUri);
   const [copyMessage, setCopyMessage] = useState("");
   const sourceInfos = React.useMemo(
      () => parseSourceInfos(data?.sourceInfos),
      [data?.sourceInfos],
   );

   // Whether the model imports other files — drives the empty-state hint for
   // import-only models, whose discovery surface is legitimately empty (no
   // local sources, no export{}) but whose page would otherwise render blank.
   // `modelDef` is returned by the server but intentionally absent from the
   // typed CompiledModel schema (internal compiler blob), hence the cast.
   const modelDefJson = (data as { modelDef?: string } | undefined)?.modelDef;
   const hasImports = React.useMemo(() => {
      if (!modelDefJson) return false;
      try {
         const def = JSON.parse(modelDefJson) as { imports?: unknown[] };
         return Array.isArray(def.imports) && def.imports.length > 0;
      } catch {
         return false;
      }
   }, [modelDefJson]);

   if (isLoading) {
      return <Loading text="Fetching Model..." />;
   }

   if (isError) {
      console.log("error", error);
      return <ApiErrorDisplay error={error} context={`Model > ${modelPath}`} />;
   }

   const copyToClipboard = () => {
      const url = window.location.href;
      navigator.clipboard
         .writeText(url)
         .then(() => setCopyMessage("URL copied to clipboard!"))
         .catch(() => setCopyMessage("Failed to copy URL"));
   };

   return (
      <>
         <Box
            sx={{
               position: "relative",
               maxWidth: "1200px",
               margin: "0 auto",
               paddingTop: "24px",
            }}
         >
            {/* Sources Section */}
            {sourceInfos.length > 0 && (
               <Stack spacing={2} component="section">
                  {/* Sources Header */}
                  <Box
                     sx={{
                        padding: "0 0 16px 0",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                     }}
                  >
                     <Typography
                        variant="h1"
                        sx={{
                           fontSize: "28px",
                           fontWeight: "600",
                           color: "#1a1a1a",
                           marginBottom: "8px",
                           marginTop: "0",
                           paddingLeft: "0",
                        }}
                     >
                        Sources
                     </Typography>
                     <Tooltip title="Copy link">
                        <IconButton
                           size="small"
                           aria-label="Copy link"
                           onClick={copyToClipboard}
                           sx={{ color: "text.secondary" }}
                        >
                           <LinkOutlinedIcon fontSize="small" />
                        </IconButton>
                     </Tooltip>
                  </Box>

                  <ModelSources sourceInfos={sourceInfos} />
               </Stack>
            )}

            {/* Named Queries Section */}
            {data?.queries?.length > 0 && (
               <Stack
                  spacing={2}
                  component="section"
                  sx={{ marginTop: "24px" }}
               >
                  {/* Named Queries Header */}
                  <Box sx={{ padding: "0 0 16px 0" }}>
                     <Typography
                        variant="h2"
                        sx={{
                           fontSize: "24px",
                           fontWeight: "600",
                           color: "#1a1a1a",
                           marginBottom: "0",
                           marginTop: "8px",
                           paddingLeft: "0",
                        }}
                     >
                        Named Queries
                     </Typography>
                  </Box>

                  {/* Render the named queries */}
                  {data.queries.map((query) => (
                     <ModelCell
                        key={query.name}
                        queryName={query.name}
                        annotations={query.annotations}
                        resourceUri={resourceUri}
                        runOnDemand={runOnDemand}
                        maxResultSize={maxResultSize}
                     />
                  ))}
               </Stack>
            )}

            {/* Empty discovery surface: nothing exported, nothing to render.
                For import-only models, say why and how to fix it — otherwise
                the page reads as broken. */}
            {sourceInfos.length === 0 &&
               !(data?.queries && data.queries.length > 0) && (
                  <Box sx={{ textAlign: "center", py: 6 }}>
                     <Typography
                        variant="body1"
                        sx={{ color: "text.secondary", marginBottom: "8px" }}
                     >
                        This model exposes no sources or queries.
                     </Typography>
                     {hasImports && (
                        <Typography variant="body2" color="text.secondary">
                           It imports other files but re-exports nothing. Add{" "}
                           <code>export {"{ source_name }"}</code> to surface
                           imported sources here.
                        </Typography>
                     )}
                  </Box>
               )}
         </Box>
         <Snackbar
            open={copyMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setCopyMessage("")}
            message={copyMessage}
         />
      </>
   );
}
