import { Divider, Typography } from "@mui/material";
import { Configuration, DefaultApi } from "../../client";
import axios from "axios";
import Markdown from "markdown-to-jsx";
import { StyledCard, StyledCardContent, StyledCardMedia } from "../styles";
import { QueryClient, useQuery } from "@tanstack/react-query";

axios.defaults.baseURL = "http://localhost:4000";
const aboutApi = new DefaultApi(new Configuration());
const queryClient = new QueryClient();

interface AboutProps {
   server?: string;
   projectName: string;
   accessToken?: string;
}

export default function About({
   server,
   projectName,
   accessToken,
}: AboutProps) {
   const { data, isSuccess, isError, error } = useQuery(
      {
         queryKey: ["about", server, projectName],
         queryFn: () =>
            aboutApi.about(projectName, {
               baseURL: server,
               withCredentials: true,
               headers: {
                  Authorization: accessToken && `Bearer ${accessToken}`,
               },
            }),
      },
      queryClient,
   );

   return (
      <>
         {!isSuccess && !isError && (
            <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
               Fetching About...
            </Typography>
         )}
         {isSuccess && (
            <StyledCard variant="outlined">
               <StyledCardContent>
                  <Typography variant="overline" fontWeight="bold">
                     Readme
                  </Typography>
                  <Divider />
               </StyledCardContent>
               <StyledCardMedia>
                  <Markdown>{data.data.readme}</Markdown>
               </StyledCardMedia>
            </StyledCard>
         )}
         {isError && (
            <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
               {error.message}
            </Typography>
         )}
      </>
   );
}
