export type ParsedResource = {
   environmentName: string;
   packageName?: string | undefined;
   connectionName?: string | undefined;
   versionId?: string | undefined;
   modelPath?: string | undefined;
};

export const parseResourceUri = (resourceUri: string) => {
   const parsedUri = new URL(resourceUri);
   const parsedResource = {} as ParsedResource;
   if (parsedUri.protocol !== "publisher:") {
      throw new Error(`Failed to parse resource URI: ${resourceUri}`);
   }
   const pathParts = (parsedUri.hostname + parsedUri.pathname).split("/");
   for (let i = 0; i < pathParts.length; i += 2) {
      const part = pathParts[i];
      if (part === "environments") {
         parsedResource.environmentName =
            decodeURI(pathParts[i + 1]) || undefined;
      } else if (part === "packages") {
         parsedResource.packageName = decodeURI(pathParts[i + 1]) || undefined;
      } else if (part === "connections") {
         parsedResource.connectionName =
            decodeURI(pathParts[i + 1]) || undefined;
      } else if (part === "models") {
         parsedResource.modelPath =
            decodeURI(pathParts.slice(i + 1).join("/")) || undefined;
      }
   }

   parsedResource.versionId =
      parsedUri.searchParams.get("versionId") || undefined;
   if (!parsedResource.environmentName) {
      throw new Error(`Failed to parse resource URI: ${resourceUri}`);
   }
   return parsedResource;
};

export const encodeResourceUri = (resource: ParsedResource) => {
   if (!resource.environmentName) {
      throw new Error(
         `Failed to encode resource URI, missing environment name: ${resource}`,
      );
   }
   let uri = `publisher://environments/${resource.environmentName}`;
   if (resource.packageName) {
      uri += `/packages/${resource.packageName}`;
   }
   if (resource.connectionName) {
      uri += `/connections/${resource.connectionName}`;
   }
   if (resource.modelPath) {
      uri += `/models/${resource.modelPath}`;
   }
   if (resource.packageName && resource.versionId) {
      uri += `?versionId=${resource.versionId}`;
   }
   return uri;
};
