import { encodeResourceUri, Model, Notebook } from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import { useParams } from "react-router-dom";

function ModelPage() {
   const params = useParams();
   const modelPath = params["*"];
   if (!params.projectName) {
      return (
         <div>
            <h2>Missing project name</h2>
         </div>
      );
   }
   if (!params.packageName) {
      return (
         <div>
            <h2>Missing package name</h2>
         </div>
      );
   }
   const resourceUri = encodeResourceUri({
      projectName: params.projectName,
      packageName: params.packageName,
      modelPath,
   });

   const wrapperSx = { p: 3, maxWidth: 1200, mx: "auto" } as const;

   if (modelPath?.endsWith(".malloy")) {
      return (
         <Box sx={wrapperSx}>
            <Model
               resourceUri={resourceUri}
               runOnDemand={true}
               maxResultSize={512 * 1024}
            />
         </Box>
      );
   }
   if (modelPath?.endsWith(".malloynb")) {
      return (
         <Box sx={wrapperSx}>
            <Notebook resourceUri={resourceUri} maxResultSize={1024 * 1024} />
         </Box>
      );
   }
   return (
      <Box sx={wrapperSx}>
         <h2>Unrecognized file type: {modelPath}</h2>
      </Box>
   );
}

export default ModelPage;
