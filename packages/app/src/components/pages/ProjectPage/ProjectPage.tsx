import { useParams } from "react-router-dom";
import { encodeResourceUri, Project } from "@malloy-publisher/sdk";
import { useRouterClickHandler } from "@malloy-publisher/sdk";

function ProjectPage() {
   const navigate = useRouterClickHandler();
   const { projectName } = useParams();
   if (!projectName) {
      return (
         <div>
            <h2>Missing project name</h2>
         </div>
      );
   } else {
      const resourceUri = encodeResourceUri({ projectName });
      return <Project onSelectPackage={navigate} resourceUri={resourceUri} />;
   }
}
export default ProjectPage;
