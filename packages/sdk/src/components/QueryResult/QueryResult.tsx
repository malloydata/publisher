import { Suspense, lazy } from "react";
import { Configuration, QueryresultsApi } from "../../client";
import axios from "axios";
import { Typography } from "@mui/material";
import { QueryClient, useQuery } from "@tanstack/react-query";

const RenderedResult = lazy(() => import("../RenderedResult/RenderedResult"));

axios.defaults.baseURL = "http://localhost:4000";
const queryResultsApi = new QueryresultsApi(new Configuration());
const queryClient = new QueryClient();

interface QueryResultProps {
   server?: string;
   projectName: string;
   packageName: string;
   modelPath: string;
   versionId?: string;
   query?: string;
   sourceName?: string;
   queryName?: string;
   accessToken?: string;
}

export default function QueryResult({
   server,
   projectName,
   packageName,
   modelPath,
   versionId,
   query,
   sourceName,
   queryName,
   accessToken,
}: QueryResultProps) {
   const { data, isSuccess, isError, error } = useQuery(
      {
         queryKey: [
            "queryResult",
            server,
            projectName,
            packageName,
            modelPath,
            versionId,
            query,
            sourceName,
            queryName,
            accessToken,
         ],
         queryFn: () =>
            queryResultsApi.executeQuery(
               projectName,
               packageName,
               modelPath,
               query,
               sourceName,
               queryName,
               versionId,
               {
                  baseURL: server,
                  withCredentials: !accessToken,
                  headers: {
                     Authorization: accessToken && `Bearer ${accessToken}`,
                  },
               },
            ),
      },
      queryClient,
   );

   return (
      <>
         {!isSuccess && !isError && (
            <Typography variant="body2" sx={{ p: "20px", m: "auto" }}>
               Fetching Query Results...
            </Typography>
         )}
         {isSuccess && (
            <Suspense fallback="Loading malloy...">
               <RenderedResult result={data.data.result} />
            </Suspense>
         )}
         {isError && (
            <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
               {`${projectName} > ${packageName} > ${modelPath} > ${versionId} - ${error.message}`}
            </Typography>
         )}
      </>
   );
}
