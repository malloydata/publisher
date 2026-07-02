import {
   encodeResourceUri,
   Materializations,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";
import { useParams } from "react-router-dom";

function MaterializationsPage() {
   const { environmentName, packageName } = useParams();
   const navigate = useRouterClickHandler();
   if (!environmentName) {
      return (
         <div>
            <h2>Missing environment name</h2>
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
         environmentName,
         packageName,
      });
      return (
         <Materializations
            onClickPackageFile={navigate}
            resourceUri={resourceUri}
         />
      );
   }
}
export default MaterializationsPage;
