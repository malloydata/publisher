import { useParams } from "react-router-dom";
import { encodeResourceUri, Package } from "@malloy-publisher/sdk";
import { useRouterClickHandler } from "@malloy-publisher/sdk";

function PackagePage() {
   const { projectName, packageName } = useParams();
   const navigate = useRouterClickHandler();
   if (!projectName) {
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
         <Package onClickPackageFile={navigate} resourceUri={resourceUri} />
      );
   }
}
export default PackagePage;
