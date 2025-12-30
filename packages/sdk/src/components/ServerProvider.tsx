import { QueryClientProvider } from "@tanstack/react-query";
import axios from "axios";
import React, {
   createContext,
   ReactNode,
   useContext,
   useMemo,
   useEffect,
   useState,
} from "react";
import {
   ConnectionsApi,
   DatabasesApi,
   ModelsApi,
   NotebooksApi,
   PackagesApi,
   ProjectsApi,
   PublisherApi,
   WatchModeApi,
} from "../client";
import { Configuration } from "../client/configuration";
import { globalQueryClient } from "../utils/queryClient";

export interface ServerContextValue {
   server: string;
   getAccessToken?: () => Promise<string>;
   apiClients: ApiClients;
   mutable: boolean;
   isLoadingStatus: boolean;
}

const ServerContext = createContext<ServerContextValue | undefined>(undefined);

export interface ServerProviderProps {
   children: ReactNode;
   /** An optional alternative base URL of the Publisher server. */
   baseURL?: string;
   /** An optional function to get an access token.
    *
    * @example
    * ```ts
    * <ServerProvider getAccessToken={async () => "Bearer 123"}>
    * ```
    * Will send "Bearer 123" in the Authorization header.
    */
   getAccessToken?: () => Promise<string>;
   /** Whether the publisher should allow project and package management operations.
    * When false, users can only view and explore existing projects and packages.
    * @default true
    */
}

const getApiClients = (
   baseURL?: string,
   accessToken?: () => Promise<string>,
) => {
   const basePath = `${window.location.protocol}//${window.location.host}/api/v0`;

   // Create a custom axios instance with proper configuration
   const axiosInstance = axios.create({
      baseURL: baseURL || basePath,
      withCredentials: true,
      timeout: 600000,
   });

   axiosInstance.interceptors.request.use(async (config) => {
      const token = await accessToken?.();
      config.headers.Authorization = token || "";
      return config;
   });

   const config = new Configuration({ basePath });

   return {
      models: new ModelsApi(config, basePath, axiosInstance),
      publisher: new PublisherApi(config, basePath, axiosInstance),
      projects: new ProjectsApi(config, basePath, axiosInstance),
      packages: new PackagesApi(config, basePath, axiosInstance),
      notebooks: new NotebooksApi(config, basePath, axiosInstance),
      connections: new ConnectionsApi(config, basePath, axiosInstance),
      databases: new DatabasesApi(config, basePath, axiosInstance),
      watchMode: new WatchModeApi(config, basePath, axiosInstance),
   };
};

export type ApiClients = ReturnType<typeof getApiClients>;

export const ServerProvider: React.FC<ServerProviderProps> = ({
   children,
   getAccessToken,
   baseURL,
}) => {
   const apiClients = useMemo(
      () => getApiClients(baseURL, getAccessToken),
      [baseURL, getAccessToken],
   );

   const server =
      baseURL || `${window.location.protocol}//${window.location.host}/api/v0`;

   const [mutable, setMutable] = useState(true);
   const [isLoadingStatus, setIsLoadingStatus] = useState(true);

   // Fetch status on mount
   useEffect(() => {
      let isMounted = true;

      const fetchStatus = async () => {
         try {
            const response = await apiClients.publisher.getStatus();

            if (isMounted) {
               const data = response.data as { frozenConfig?: boolean };
               const frozenConfig = data?.frozenConfig;
               setMutable(!frozenConfig);
               setIsLoadingStatus(false);
            }
         } catch (error) {
            console.error("Failed to fetch publisher status:", error);
            if (isMounted) {
               setMutable(true); // Default to mutable on error
               setIsLoadingStatus(false);
            }
         }
      };

      fetchStatus();

      return () => {
         isMounted = false;
      };
   }, [apiClients]);

   const value: ServerContextValue = {
      server,
      getAccessToken,
      apiClients,
      mutable,
      isLoadingStatus,
   };

   return (
      <QueryClientProvider client={globalQueryClient}>
         <ServerContext.Provider value={value}>
            {children}
         </ServerContext.Provider>
      </QueryClientProvider>
   );
};

export const useServer = (): ServerContextValue => {
   const context = useContext(ServerContext);
   if (context === undefined) {
      throw new Error("useServer must be used within a ServerProvider");
   }
   return context;
};
