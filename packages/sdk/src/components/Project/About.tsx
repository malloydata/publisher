import { Box, Button } from "@mui/material";
import Markdown from "markdown-to-jsx";
import { useState } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import {
   PackageCard,
   PackageCardContent,
   PackageSectionTitle,
} from "../styles";
interface AboutProps {
   resourceUri: string;
}

export default function About({ resourceUri }: AboutProps) {
   const { projectName } = parseResourceUri(resourceUri);
   const { apiClients } = useServer();
   const [expanded, setExpanded] = useState(false);
   const wordLimit = 90;

   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["about", projectName],
      queryFn: () => apiClients.projects.getProject(projectName, false),
   });

   const readmeContent = data?.data?.readme || "";
   const words = readmeContent.split(/\s+/);
   const shouldTruncate = words.length > wordLimit;
   const preview = words.slice(0, wordLimit).join(" ");

   return (
      <>
         {!isSuccess && !isError && <Loading text="Fetching About..." />}
         {isSuccess && readmeContent && (
            <PackageCard>
               <PackageCardContent>
                  <PackageSectionTitle>Readme</PackageSectionTitle>
                  <Box sx={{ mt: 1, fontSize: "0.875rem", lineHeight: 1.6 }}>
                     <Markdown
                        options={{
                           overrides: {
                              h1: {
                                 component: "p",
                                 props: {
                                    style: {
                                       fontSize: "inherit",
                                       fontWeight: "italic",
                                    },
                                 },
                              },
                              h2: {
                                 component: "p",
                                 props: {
                                    style: {
                                       fontSize: "inherit",
                                       fontWeight: "italic",
                                    },
                                 },
                              },
                              h3: {
                                 component: "p",
                                 props: {
                                    style: {
                                       fontSize: "inherit",
                                       fontWeight: "italic",
                                    },
                                 },
                              },
                           },
                        }}
                     >
                        {expanded || !shouldTruncate ? readmeContent : preview}
                     </Markdown>

                     {shouldTruncate && (
                        <Box sx={{ mt: 1 }}>
                           {" "}
                           {/* separate line */}
                           <Button
                              variant="text"
                              size="small"
                              onClick={() => setExpanded(!expanded)}
                           >
                              {expanded ? "Read less" : "Read more"}
                           </Button>
                        </Box>
                     )}
                  </Box>
               </PackageCardContent>
            </PackageCard>
         )}
         {isError && (
            <ApiErrorDisplay error={error} context={`${projectName} > About`} />
         )}
      </>
   );
}
