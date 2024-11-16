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
   packageName: string;
   modelPath: string;
   versionId?: string;
   query?: string;
   sourceName?: string;
   queryName?: string;
}

export default function QueryResult({
   server,
   packageName,
   modelPath,
   versionId,
   query,
   sourceName,
   queryName,
}: QueryResultProps) {
   const { data, isSuccess, isError, error } = useQuery(
      {
         queryKey: [
            "queryResult",
            server,
            packageName,
            modelPath,
            versionId,
            query,
            sourceName,
            queryName,
         ],
         queryFn: () =>
            queryResultsApi.executeQuery(
               packageName,
               modelPath,
               versionId,
               query,
               sourceName,
               queryName,
               {
                  baseURL: server,
                  withCredentials: true,
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
               <RenderedResult
                  queryResult={data.data.queryResult}
                  modelDef={data.data.modelDef}
                  dataStyles={data.data.dataStyles}
               />
            </Suspense>
         )}
         {isError && (
            <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
               {`${packageName} > ${modelPath} > ${versionId} - ${error.message}`}
            </Typography>
         )}
      </>
   );
}