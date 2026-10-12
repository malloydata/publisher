// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Lightweight client entry point for better code splitting
// This module contains essential client functionality without heavy UI components

// Export OpenAPI generated client APIs
export * from "./client/api";
export * from "./client/configuration";

// Export server provider and hooks for React integration
export { ServerProvider, useServer } from "./components/ServerProvider";
export type {
   ApiClients,
   ServerContextValue,
   ServerProviderProps,
} from "./components/ServerProvider";

// Export the query client for users who need direct access
export { globalQueryClient } from "./utils/queryClient";

// The theme hook a host reads at its root, to follow or set the SDK's light or
// dark mode. It lives beside `ServerProvider`, which mounts its provider, so
// taking it from here costs nothing the provider has not already loaded; from
// the main entry it would put the dashboard, explorer and renderer code on the
// host's critical path.
export { usePublisherTheme } from "./theme/ThemeContext";
export type { ResolvedTheme, Theme, ThemeMode } from "./theme/types";
