// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

export { default as RenderedResult } from "./RenderedResult";
export { default as ResultContainer } from "./ResultContainer";
export { ResultPanel, type ResultPanelProps } from "./ResultPanel";
export {
   contentNodeDepth,
   INITIAL_RENDER_HEIGHT,
   initialResultHeight,
   measureContentHeight,
   MODEL_CELL_MAX_HEIGHT,
   NOTEBOOK_CELL_MAX_HEIGHT,
   remeasuresAfterReady,
   resolveResultHeight,
   RESULTS_DIALOG_MAX_HEIGHT,
   resultSizing,
   TILE_MAX_HEIGHT,
   UNCAPPED_CONTAINER_HEIGHT,
   type ResultSizing,
} from "./resultSizing";
