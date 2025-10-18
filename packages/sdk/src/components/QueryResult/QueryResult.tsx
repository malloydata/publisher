import { Suspense, lazy } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";

const RenderedResult = lazy(() => import("../RenderedResult/RenderedResult"));

interface QueryResultProps {
   query?: string;
   sourceName?: string;
   queryName?: string;
   resourceUri?: string;
}

export function createEmbeddedQueryResult(props: QueryResultProps): string {
   const {
      projectName: optionalProjectName,
      packageName: optionalPackageName,
   } = parseResourceUri(props.resourceUri);
   if (!optionalProjectName || !optionalPackageName) {
      throw new Error(
         "Project and Package name must be provided for query embedding.",
      );
   }
   return JSON.stringify({
      ...props,
   });
}

/**
 * This is a helper function to render a query result that is embedded as a string.
 */
export function EmbeddedQueryResult({
   embeddedQueryResult,
}: {
   embeddedQueryResult: string;
}): React.ReactElement {
   const { query, sourceName, queryName, resourceUri } = JSON.parse(
      embeddedQueryResult,
   ) as QueryResultProps;
   const { modelPath } = parseResourceUri(resourceUri);
   if (
      !modelPath ||
      (!query && (!queryName || !sourceName)) ||
      typeof modelPath !== "string"
   ) {
      throw new Error("Invalid embedded query result: " + embeddedQueryResult);
   }
   return (
      <QueryResult
         query={query}
         sourceName={sourceName}
         queryName={queryName}
         resourceUri={resourceUri}
      />
   );
}

export default function QueryResult({
   query,
   sourceName,
   queryName,
   resourceUri,
}: QueryResultProps) {
   const { modelPath, projectName, packageName, versionId } =
      parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   if (!projectName || !packageName) {
      throw new Error(
         "No project or package name provided. A resource URI must be provided.",
      );
   }

   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: [resourceUri, query, sourceName, queryName],
      queryFn: () =>
         apiClients.models.executeQueryModel(
            projectName,
            packageName,
            modelPath,
            {
               query: query,
               sourceName: sourceName,
               queryName: queryName,
               versionId: versionId,
            },
         ),
   });

   return (
      <>
         {!isSuccess && !isError && (
            <Loading text="Fetching Query Results..." />
         )}
         {isSuccess && (
            <Suspense fallback={<div>Loading...</div>}>
               <RenderedResult result={data.data.result} />
            </Suspense>
         )}
         {isError && (
            <ApiErrorDisplay
               context={`${projectName} > ${packageName} > ${modelPath}`}
               error={error}
            />
         )}
      </>
   );
}
