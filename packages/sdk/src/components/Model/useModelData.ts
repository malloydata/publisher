import { CompiledModel } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";

/**
 * Custom hook for fetching model data. Combines usePackage context with
 * useQueryWithApiError to fetch a compiled model.
 */
export function useModelData(resourceUri: string, enabled: boolean = true) {
   const { modelPath, environmentName, packageName, versionId } =
      parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   return useQueryWithApiError<CompiledModel>({
      queryKey: ["package", environmentName, packageName, modelPath, versionId],
      queryFn: async () => {
         const response = await apiClients.models.getModel(
            environmentName,
            packageName,
            modelPath,
            versionId,
         );
         return response.data;
      },
      enabled: enabled,
   });
}
