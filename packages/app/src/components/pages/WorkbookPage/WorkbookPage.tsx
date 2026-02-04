import { encodeResourceUri, Workbook } from "@malloy-publisher/sdk";
import { useParams } from "react-router-dom";

function WorkbookPage() {
   const { workspace, workbookPath, projectName, packageName } = useParams();
   if (!workspace) {
      return (
         <div>
            <h2>Missing workspace</h2>
         </div>
      );
   } else if (!workbookPath) {
      return (
         <div>
            <h2>Missing workbook path</h2>
         </div>
      );
   } else if (!projectName) {
      return (
         <div>
            <h2>Missing project name</h2>
         </div>
      );
   } else if (!packageName) {
      return (
         <div>
            <h2>Missing package name</h2>
         </div>
      );
   } else {
      const resourceUri = encodeResourceUri({
         projectName,
         packageName,
      });
      return (
         <Workbook
            key={`${workbookPath}`}
            workbookPath={{ path: workbookPath, workspace: workspace }}
            resourceUri={resourceUri}
         />
      );
   }
}
export default WorkbookPage;
