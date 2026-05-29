import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import axios from "axios";
import React, {
   createContext,
   ReactNode,
   useCallback,
   useContext,
   useEffect,
   useMemo,
   useState,
} from "react";
import {
   ConnectionsApi,
   DatabasesApi,
   EnvironmentsApi,
   ModelsApi,
   NotebooksApi,
   PackagesApi,
   PublisherApi,
   WatchModeApi,
} from "../client";
import { Configuration } from "../client/configuration";
import { resolveMode } from "../theme/resolveTheme";
import { ThemeProvider } from "../theme/ThemeContext";
import type { Theme, ThemeMode } from "../theme/types";
import { globalQueryClient } from "../utils/queryClient";

const THEME_MODE_STORAGE_KEY = "publisher:themeMode";

/** SSR-safe read of the viewer's persisted mode choice. */
function readStoredMode(): ThemeMode | "auto" | undefined {
   if (typeof window === "undefined") return undefined;
   try {
      const raw = window.localStorage.getItem(THEME_MODE_STORAGE_KEY);
      if (raw === "light" || raw === "dark" || raw === "auto") return raw;
   } catch {
      // localStorage can throw (Safari private mode, sandboxed iframes).
   }
   return undefined;
}

function writeStoredMode(mode: ThemeMode | "auto" | undefined) {
   if (typeof window === "undefined") return;
   try {
      if (mode === undefined) {
         window.localStorage.removeItem(THEME_MODE_STORAGE_KEY);
      } else {
         window.localStorage.setItem(THEME_MODE_STORAGE_KEY, mode);
      }
   } catch {
      // Persistence is best-effort; swallow.
   }
}

function getPrefersDark(): boolean {
   if (typeof window === "undefined" || !window.matchMedia) return false;
   return window.matchMedia("(prefers-color-scheme: dark)").matches;
}

// There's a bug in the OpenAPI generator that causes it to ignore baseURL in
// the axios request if axios.defaults.baseURL is not set. The per-instance
// baseURL on the custom axios instance below is the real value we use; this
// sentinel exists only to satisfy the generated client's code path.
if (!axios.defaults.baseURL) {
   axios.defaults.baseURL = "IfYouAreSeeingThis_baseURL_IsNotSet";
}

export interface ServerContextValue {
   server: string;
   getAccessToken?: () => Promise<string>;
   apiClients: ApiClients;
   mutable: boolean;
   isLoadingStatus: boolean;
   /**
    * Instance-wide default theme, pulled from the `/api/v0/status` response.
    * `undefined` while loading, or when the operator has not configured a
    * theme in `publisher.config.json`.
    */
   instanceTheme?: Theme;
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
   /** Whether the publisher should allow environment and package management operations.
    * When false, users can only view and explore existing environments and packages.
    * @default true
    */
   mutable?: boolean;
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
      environments: new EnvironmentsApi(config, basePath, axiosInstance),
      packages: new PackagesApi(config, basePath, axiosInstance),
      notebooks: new NotebooksApi(config, basePath, axiosInstance),
      connections: new ConnectionsApi(config, basePath, axiosInstance),
      databases: new DatabasesApi(config, basePath, axiosInstance),
      watchMode: new WatchModeApi(config, basePath, axiosInstance),
   };
};

export type ApiClients = ReturnType<typeof getApiClients>;

/**
 * Outer wrapper that owns the QueryClient. The inner component runs
 * react-query hooks for /status, so it has to live below the
 * QueryClientProvider.
 */
export const ServerProvider: React.FC<ServerProviderProps> = (props) => {
   return (
      <QueryClientProvider client={globalQueryClient}>
         <ServerProviderInner {...props} />
      </QueryClientProvider>
   );
};

const ServerProviderInner: React.FC<ServerProviderProps> = ({
   children,
   getAccessToken,
   baseURL,
   mutable: mutableProp,
}) => {
   const apiClients = useMemo(
      () => getApiClients(baseURL, getAccessToken),
      [baseURL, getAccessToken],
   );

   const server =
      baseURL || `${window.location.protocol}//${window.location.host}/api/v0`;

   // Fetch /status via react-query so callers like the Theme Editor can
   // invalidate the "status" key after a write and pick up the new
   // instanceTheme without a full page reload.
   const statusQuery = useQuery({
      queryKey: ["status"],
      queryFn: async () => {
         const response = await apiClients.publisher.getStatus();
         return response.data as {
            frozenConfig?: boolean;
            theme?: Theme;
         };
      },
   });

   if (statusQuery.error) {
      console.error("Failed to fetch publisher status:", statusQuery.error);
   }

   const frozenConfig = statusQuery.data?.frozenConfig;
   const instanceTheme = statusQuery.data?.theme;
   const isLoadingStatus = statusQuery.isLoading;

   // Preserve original semantics: while loading or on error, mirror the
   // mutableProp the caller passed in (which itself may be undefined).
   // Once /status arrives, frozenConfig forces read-only; otherwise the
   // explicit prop wins, with a default of true.
   let mutable: boolean | undefined;
   if (statusQuery.isLoading || statusQuery.error) {
      mutable = mutableProp;
   } else if (frozenConfig) {
      mutable = false;
   } else {
      mutable = mutableProp ?? true;
   }

   // Stable layers reference so ThemeProvider's useMemo doesn't reshuffle
   // on every render when instanceTheme is unchanged.
   const themeLayers = useMemo(() => [instanceTheme], [instanceTheme]);

   // Viewer's light/dark/auto preference. localStorage is the source of
   // truth across reloads; matchMedia provides the OS hint that 'auto'
   // resolves against.
   const [userChoice, setUserChoice] = useState<ThemeMode | "auto" | undefined>(
      readStoredMode,
   );
   const [prefersDark, setPrefersDark] = useState<boolean>(getPrefersDark);

   useEffect(() => {
      if (typeof window === "undefined" || !window.matchMedia) return;
      const media = window.matchMedia("(prefers-color-scheme: dark)");
      const listener = (e: MediaQueryListEvent) => setPrefersDark(e.matches);
      media.addEventListener("change", listener);
      return () => media.removeEventListener("change", listener);
   }, []);

   const setMode = useCallback((next: ThemeMode | "auto") => {
      setUserChoice(next);
      writeStoredMode(next);
   }, []);

   // When the operator forbids viewer overrides, ignore localStorage and
   // honour only the instance defaultMode (with the OS preference for
   // 'auto'). The toggle UI hides itself in this case, but this guard
   // protects against an old localStorage entry from before the operator
   // locked it.
   const allowUserToggle = instanceTheme?.allowUserToggle ?? true;
   const effectiveUserChoice = allowUserToggle ? userChoice : undefined;
   const mode: ThemeMode = useMemo(
      () =>
         resolveMode(
            instanceTheme?.defaultMode,
            effectiveUserChoice,
            prefersDark,
         ),
      [instanceTheme?.defaultMode, effectiveUserChoice, prefersDark],
   );

   const value: ServerContextValue = {
      server,
      getAccessToken,
      apiClients,
      mutable,
      isLoadingStatus,
      instanceTheme,
   };

   return (
      <ServerContext.Provider value={value}>
         <ThemeProvider
            layers={themeLayers}
            mode={mode}
            userChoice={effectiveUserChoice}
            setMode={setMode}
            allowUserToggle={allowUserToggle}
         >
            {children}
         </ThemeProvider>
      </ServerContext.Provider>
   );
};

export const useServer = (): ServerContextValue => {
   const context = useContext(ServerContext);
   if (context === undefined) {
      throw new Error("useServer must be used within a ServerProvider");
   }
   return context;
};
