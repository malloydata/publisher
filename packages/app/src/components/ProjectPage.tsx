import { useNavigate, useParams } from "react-router-dom";
import { Project } from "@malloy-publisher/sdk";

interface ProjectPageProps {
   server?: string;
}

export function ProjectPage({ server }: ProjectPageProps) {
   const navigate = useNavigate();
   const { projectName } = useParams();
   if (!projectName) {
      return (
         <div>
            <h2>Missing project name</h2>
         </div>
      );
   } else {
      return (
         <Project
            server={server}
            projectName={projectName}
            navigate={navigate}
         />
      );
   }
}
