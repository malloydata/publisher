// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { initMcpApp } from "../shared/mcp_app_init";
import {
   NO_PAYLOAD_AGENT,
   NO_PAYLOAD_HUMAN,
   RESULT_TOO_LARGE_AGENT,
   RESULT_TOO_LARGE_HUMAN,
} from "./messages";
import { renderError, renderResult } from "./renderer";

const root = document.getElementById("root")!;

let toolInput: Record<string, unknown> | null = null;
let toolOutput: Record<string, unknown> | null = null;
let hasRendered = false;

// Some hosts (Claude Desktop among them) do not guarantee that tool_input
// arrives before tool_result. Rendering on output alone would silently lose
// input-driven behaviour such as `expanded`. So when output arrives first, wait
// briefly for input, and render anyway if it never comes.
let toolInputWaitTimer: ReturnType<typeof setTimeout> | null = null;
const TOOL_INPUT_WAIT_MS = 100;

const app = initMcpApp("Malloy Query Result", "1.0.0", {
   onToolInput: (args) => {
      toolInput = args;
      tryRender();
   },

   onToolResult: (payload) => {
      try {
         toolOutput = JSON.parse(payload.text);
      } catch {
         // A payload that will not parse ~always means the host truncated an
         // oversized result. Tell the human how to shrink it, and hand the agent
         // something it can act on so it recovers by re-running smaller.
         failCard(RESULT_TOO_LARGE_HUMAN, RESULT_TOO_LARGE_AGENT);
         return;
      }
      if (!toolInput && toolInputWaitTimer === null) {
         toolInputWaitTimer = setTimeout(() => {
            toolInputWaitTimer = null;
            tryRender();
         }, TOOL_INPUT_WAIT_MS);
      }
      tryRender();
   },

   // Distinct from an unparseable payload, and worded differently: nothing was
   // returned to render, so telling the user to shrink the query would send them
   // after a problem they do not have.
   onMissingPayload: () => failCard(NO_PAYLOAD_HUMAN, NO_PAYLOAD_AGENT),
});

/** Show a failure to the human and report the same case to the agent once. */
function failCard(humanMessage: string, agentMessage: string) {
   if (hasRendered) return;
   hasRendered = true;
   renderError(root, humanMessage);
   app.sendMessage({
      role: "user",
      content: [{ type: "text", text: agentMessage }],
   }).catch(() => {});
}

function tryRender() {
   if (hasRendered || !toolOutput) return;
   if (!toolInput && toolInputWaitTimer !== null) return;

   if (toolInputWaitTimer !== null) {
      clearTimeout(toolInputWaitTimer);
      toolInputWaitTimer = null;
   }
   hasRendered = true;

   if ("error" in toolOutput) {
      renderError(
         root,
         String(toolOutput.error),
         toolInput?.query as string | undefined,
      );
      return;
   }
   renderResult(root, toolOutput, toolInput);
}
