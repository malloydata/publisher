// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

export * from "./components";
export { default as ConnectionExplorer } from "./components/Environment/ConnectionExplorer";
export { useServer } from "./components/ServerProvider";
export * from "./hooks";
export { useRawQueryData } from "./hooks/useRawQueryData";
export * from "./theme";
// The Console's write events, and the seam a host installs a sink into. Set
// once at boot: the dialogs that write are not mounted by the host, so there
// is nothing to hand a handler to.
export {
   setConsoleEventHandler,
   type ConsoleEvent,
   type ConsoleEventHandler,
   type ConsoleResource,
} from "./telemetry/consoleEvents";
export * from "./utils/formatting";
// How a package's own files are addressed. Exported because a host that renders
// its own "this path is not a model" state has to point at the URL that does
// serve the file, and this is the one place that string is built.
export { packageFileUrl } from "./utils/dataAppEmbed";
export * from "./constants/docLinks";
