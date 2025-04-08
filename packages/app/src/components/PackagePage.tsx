import { useParams, useNavigate } from "react-router-dom";
import { Package } from "@malloy-publisher/sdk";

interface PackagePageProps {
   server?: string;
}

export function PackagePage({ server }: PackagePageProps) {
   const { projectName, packageName } = useParams();
   const navigate = useNavigate();
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
      return (
         <Package
            server={server}
            projectName={projectName}
            packageName={packageName}
            navigate={navigate}
         />
      );
   }
}
