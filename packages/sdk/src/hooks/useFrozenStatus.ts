import { useQueryWithApiError } from "./useQueryWithApiError";
import { useServer } from "../components/ServerProvider";
import { ApiError } from "../components/ApiErrorDisplay";

interface FrozenStatus {
   mutable: boolean;
   frozenConfig: boolean;
}

export const useFrozenStatus = () => {
   const { server } = useServer();

   return useQueryWithApiError<FrozenStatus, ApiError>({
      queryKey: ["frozen-status"],
      queryFn: async () => {
         const response = await fetch(`${server}/frozen-status`);
         if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
         }
         return response.json();
      },
      staleTime: 1000 * 60 * 5,
      refetchOnWindowFocus: false,
   });
};

// Convenience hook that just returns the mutable boolean
export const useIsMutable = (): {
   mutableConfig: boolean;
   isLoading: boolean;
} => {
   const { data, isLoading } = useFrozenStatus();

   return {
      mutableConfig: data?.mutable ?? true,
      isLoading,
   };
};
