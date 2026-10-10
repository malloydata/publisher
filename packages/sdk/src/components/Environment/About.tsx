// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import { useState } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { PackageSection } from "../PackageSection";
import { Prose } from "../Prose";
import { SecondaryButton } from "../buttons";
import { useServer } from "../ServerProvider";
interface AboutProps {
   resourceUri: string;
}

export default function About({ resourceUri }: AboutProps) {
   const { environmentName } = parseResourceUri(resourceUri);
   const { apiClients } = useServer();
   const [expanded, setExpanded] = useState(false);
   const wordLimit = 90;

   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["about", environmentName],
      queryFn: () =>
         apiClients.environments.getEnvironment(environmentName, false),
   });

   const readmeContent = data?.data?.readme || "";
   const words = readmeContent.split(/\s+/);
   const shouldTruncate = words.length > wordLimit;
   const preview = words.slice(0, wordLimit).join(" ");

   return (
      <>
         {!isSuccess && !isError && <Loading text="Fetching About..." />}
         {isSuccess && readmeContent && (
            <PackageSection title="Readme">
               <Prose variant="caption">
                  {expanded || !shouldTruncate ? readmeContent : preview}
               </Prose>
               {shouldTruncate && (
                  <Box sx={{ mt: 1 }}>
                     <SecondaryButton
                        label={expanded ? "Read less" : "Read more"}
                        onClick={() => setExpanded(!expanded)}
                     />
                  </Box>
               )}
            </PackageSection>
         )}
         {isError && (
            <ApiErrorDisplay
               error={error}
               context={`${environmentName} > About`}
            />
         )}
      </>
   );
}
