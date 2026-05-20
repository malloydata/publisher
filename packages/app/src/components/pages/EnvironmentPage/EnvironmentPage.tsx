import {
   encodeResourceUri,
   Environment,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";
import { useParams } from "react-router-dom";

function EnvironmentPage() {
   const navigate = useRouterClickHandler();
   const { environmentName } = useParams();
   if (!environmentName) {
      return (
         <div>
            <h2>Missing environment name</h2>
         </div>
      );
   } else {
      const resourceUri = encodeResourceUri({ environmentName });
      return (
         <Environment onSelectPackage={navigate} resourceUri={resourceUri} />
      );
   }
}
export default EnvironmentPage;
