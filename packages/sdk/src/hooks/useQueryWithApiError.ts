import {
   useMutation,
   UseMutationOptions,
   UseMutationResult,
   useQuery,
   UseQueryOptions,
   UseQueryResult,
} from "@tanstack/react-query";
import { useServer } from "../components";
import { ApiError } from "../components/ApiErrorDisplay";
import { globalQueryClient } from "../utils/queryClient";

// Re-export the global query client for backward compatibility
export { globalQueryClient };
export function useQueryWithApiError<TData = unknown, TError = ApiError>(
   options: Omit<UseQueryOptions<TData, TError>, "throwOnError" | "retry"> & {
      queryFn: () => Promise<TData>;
   },
): UseQueryResult<TData, TError> {
   const { server } = useServer();
   const enhancedOptions: UseQueryOptions<TData, TError> = {
      ...options,
      // Add in the server to the query key so that we can have a per-server caches.
      queryKey: [...options.queryKey, server],
      queryFn: async () => {
         try {
            return await options.queryFn();
         } catch (err) {
            // Standardized error handling for axios errors
            if (err && typeof err === "object" && "response" in err) {
               const axiosError = err as {
                  response?: {
                     status: number;
                     data: { code: number; message: string };
                  };
                  message: string;
               };
               if (axiosError.response?.data) {
                  const apiError: ApiError = new Error(
                     axiosError.response.data.message || axiosError.message,
                  );
                  apiError.status = axiosError.response.status;
                  apiError.data = axiosError.response.data;
                  throw apiError;
               }
            }
            // For other errors, throw as is
            throw err;
         }
      },
      retry: false,
      throwOnError: false,
   };

   return useQuery(enhancedOptions, globalQueryClient);
}

export function useMutationWithApiError<
   TData = unknown,
   TError = ApiError,
   TVariables = void,
>(
   options: Omit<
      UseMutationOptions<TData, TError, TVariables>,
      "throwOnError" | "retry" | "mutationFn"
   > & {
      mutationFn: (variables: TVariables) => Promise<TData>;
   },
): UseMutationResult<TData, TError, TVariables> {
   const enhancedOptions: UseMutationOptions<TData, TError, TVariables> = {
      ...options,
      mutationFn: async (variables: TVariables) => {
         try {
            return await options.mutationFn(variables);
         } catch (err) {
            // Standardized error handling for axios errors
            if (err && typeof err === "object" && "response" in err) {
               const axiosError = err as {
                  response?: {
                     status: number;
                     data: { code: number; message: string };
                  };
                  message: string;
               };
               if (axiosError.response?.data) {
                  const apiError: ApiError = new Error(
                     axiosError.response.data.message || axiosError.message,
                  );
                  apiError.status = axiosError.response.status;
                  apiError.data = axiosError.response.data;
                  throw apiError;
               }
            }
            // For other errors, throw as is
            throw err;
         }
      },
      retry: false,
      throwOnError: false,
   };

   return useMutation(enhancedOptions, globalQueryClient);
}
