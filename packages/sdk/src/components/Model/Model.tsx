import "@malloydata/malloy-explorer/styles.css";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import ZoomOutMapIcon from "@mui/icons-material/ZoomOutMap";
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
import { ModelExplorer } from "./ModelExplorer";
import { ModelExplorerDialog } from "./ModelExplorerDialog";
import { QueryExplorerResult } from "./SourcesExplorer";
import { useModelData } from "./useModelData";

interface ModelProps {
   onChange?: (query: QueryExplorerResult) => void;
   resourceUri: string;
   runOnDemand?: boolean;
   maxResultSize?: number;
}

// Note: For this to properly render outside of publisher,
// you must explicitly import the styles from the package:
// import "@malloy-publisher/sdk/malloy-explorer.css";

export default function Model({
   onChange,
   resourceUri,
   runOnDemand = false,
   maxResultSize = 0,
}: ModelProps) {
   const { modelPath } = parseResourceUri(resourceUri);
   const { data, isError, isLoading, error } = useModelData(resourceUri);
   const [dialogOpen, setDialogOpen] = React.useState(false);
   const [sharedQuery, setSharedQuery] = React.useState<
      QueryExplorerResult | undefined
   >();
   const [sharedSourceIndex, setSharedSourceIndex] = React.useState(0);
   const [copyMessage, setCopyMessage] = useState("");

   if (isLoading) {
      return <Loading text="Fetching Model..." />;
   }

   if (isError) {
      console.log("error", error);
      return <ApiErrorDisplay error={error} context={`Model > ${modelPath}`} />;
   }

   // Shared handlers for both embedded and dialog explorers
   const handleQueryChange = (query: QueryExplorerResult) => {
      setSharedQuery(query);
      if (onChange) {
         onChange(query);
      }
   };

   const handleSourceChange = (index: number) => {
      setSharedSourceIndex(index);
   };

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
            {Array.isArray(data?.sourceInfos) &&
               data.sourceInfos.length > 0 && (
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
                        <Tooltip title="Click to copy link">
                           <LinkOutlinedIcon
                              sx={{
                                 fontSize: "24px",
                                 color: "#666666",
                                 cursor: "pointer",
                              }}
                              onClick={copyToClipboard}
                           />
                        </Tooltip>
                     </Box>

                     <ModelExplorer
                        data={data}
                        onChange={handleQueryChange}
                        onSourceChange={handleSourceChange}
                        existingQuery={sharedQuery}
                        initialSelectedSourceIndex={sharedSourceIndex}
                        resourceUri={resourceUri}
                     />

                     {/* Magnifying glass icon */}
                     <IconButton
                        sx={{
                           position: "absolute",
                           top: "90px",
                           right: "4px",
                           backgroundColor: "rgba(255, 255, 255, 0.9)",
                           "&:hover": {
                              backgroundColor: "rgba(255, 255, 255, 1)",
                           },
                           width: "32px",
                           height: "32px",
                           zIndex: 2,
                        }}
                        onClick={() => setDialogOpen(true)}
                     >
                        <Tooltip title="Expanded view">
                           <ZoomOutMapIcon
                              sx={{
                                 fontSize: "18px",
                                 color: "#666666",
                                 marginBottom: "5px",
                              }}
                           />
                        </Tooltip>
                     </IconButton>
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

            {/* Model Explorer Dialog */}
            <ModelExplorerDialog
               open={dialogOpen}
               onClose={() => setDialogOpen(false)}
               resourceUri={resourceUri}
               data={data}
               title={`Model: ${modelPath.split("/").pop()}`}
               existingQuery={sharedQuery}
               initialSelectedSourceIndex={sharedSourceIndex}
               onChange={handleQueryChange}
               onSourceChange={handleSourceChange}
            />
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
