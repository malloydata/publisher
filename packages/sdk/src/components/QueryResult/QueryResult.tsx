// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQueryResult } from "../../hooks/useQueryResult";
import { parseResourceUri } from "../../utils/formatting";
import { ResultPanel } from "../RenderedResult/ResultPanel";

interface QueryResultProps {
   query?: string;
   sourceName?: string;
   queryName?: string;
   resourceUri?: string;
   height?: number;
}

export function createEmbeddedQueryResult(props: QueryResultProps): string {
   const {
      environmentName: optionalProjectName,
      packageName: optionalPackageName,
   } = parseResourceUri(props.resourceUri);
   if (!optionalProjectName || !optionalPackageName) {
      throw new Error(
         "Environment and Package name must be provided for query embedding.",
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
   const { query, sourceName, queryName, resourceUri, height } = JSON.parse(
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
         height={height}
      />
   );
}

export default function QueryResult({
   query,
   sourceName,
   queryName,
   resourceUri,
   height = 400,
}: QueryResultProps) {
   const { modelPath, environmentName, packageName, versionId } =
      parseResourceUri(resourceUri);

   if (!environmentName || !packageName) {
      throw new Error(
         "No environment or package name provided. A resource URI must be provided.",
      );
   }

   const state = useQueryResult({
      environmentName,
      packageName,
      modelPath,
      versionId,
      query,
      sourceName,
      queryName,
   });

   return (
      <ResultPanel
         state={state}
         context={`${environmentName} > ${packageName} > ${modelPath}`}
         maxHeight={height}
      />
   );
}
