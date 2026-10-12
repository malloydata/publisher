// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Text and retry helpers with no dependencies, apart from the main entry: a host reads a
// file's artifact tag here without loading MUI or the Malloy parser, and in a
// plain node environment. Nothing here may import a package or another module.
export {
   artifactTag,
   splitSourceLines,
   type ArtifactTag,
} from "./utils/malloyText";
export { canRetryRequest } from "./components/DocumentCreate/canRetry";
