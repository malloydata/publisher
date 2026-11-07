// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The entry of the `show_report` panel: an MCP App (io.modelcontextprotocol/ui)
// that a host such as Claude Desktop loads from `ui://publisher/report.html`.
// Built on its own (vite.mcp-app.config.ts) to `dist/mcp-app/report.js`, which
// the server serves from its REST origin, the one resource domain the panel
// declares.

import { App } from "@modelcontextprotocol/ext-apps";
import type { ThemeMode } from "@malloy-publisher/sdk";
import * as React from "react";
import * as ReactDOM from "react-dom/client";
import { type ReportApiAnswer, type ReportApiCall } from "./reportApiAdapter";
import { ReportPanel, type PanelTarget } from "./ReportPanel";
import "@malloy-publisher/sdk/styles.css";

// Read while the script runs: `currentScript` is null after that.
const origin = (() => {
   const src = (document.currentScript as HTMLScriptElement | null)?.src;
   try {
      return src ? new URL(src).origin : window.location.origin;
   } catch {
      return window.location.origin;
   }
})();

function targetOf(
   args: Record<string, unknown> | undefined,
): PanelTarget | undefined {
   if (!args) return undefined;
   const { environmentName, packageName, path, givens } = args;
   if (
      typeof environmentName !== "string" ||
      typeof packageName !== "string" ||
      typeof path !== "string"
   ) {
      return undefined;
   }
   return {
      environmentName,
      packageName,
      path,
      givens:
         givens && typeof givens === "object"
            ? (Object.fromEntries(
                 Object.entries(givens).filter(
                    ([, v]) => typeof v === "string",
                 ),
              ) as Record<string, string>)
            : {},
   };
}

function textOf(content: unknown): string | undefined {
   if (!Array.isArray(content)) return undefined;
   const texts = content
      .filter((c): c is { type: "text"; text: string } => c?.type === "text")
      .map((c) => c.text);
   return texts.length > 0 ? texts.join("\n") : undefined;
}

const modeOf = (theme: unknown): ThemeMode | undefined =>
   theme === "dark" || theme === "light" ? theme : undefined;

/** What the host has said so far: which report, or why not, and its theme. */
interface PanelState {
   target?: PanelTarget;
   error?: string;
   mode?: ThemeMode;
}

/**
 * The host's notifications, kept outside React so the handlers are on the
 * `App` before `connect()`: a tool input or result sent before the first
 * render would otherwise be missed.
 */
function panelStore(app: App) {
   let state: PanelState = {};
   const listeners = new Set<() => void>();
   const set = (next: Partial<PanelState>) => {
      state = { ...state, ...next };
      listeners.forEach((l) => l());
   };
   // The arguments arrive before the result, so the report starts loading
   // while show_report is still answering; the result confirms it, or says
   // why not.
   app.ontoolinput = (params) => {
      const target = targetOf(params.arguments);
      if (target) set({ target, error: undefined });
   };
   app.ontoolresult = (result) => {
      if (result.isError) {
         set({
            error: textOf(result.content) ?? "The report could not be opened.",
         });
         return;
      }
      const target = targetOf(result.structuredContent);
      if (target) set({ target, error: undefined });
   };
   app.onhostcontextchanged = (context) => {
      if (context.theme) set({ mode: modeOf(context.theme) });
   };
   return {
      get: () => state,
      set,
      subscribe: (listener: () => void) => {
         listeners.add(listener);
         return () => listeners.delete(listener);
      },
   };
}

function Panel({
   app,
   store,
}: {
   app: App;
   store: ReturnType<typeof panelStore>;
}) {
   const { target, error, mode } = React.useSyncExternalStore(
      store.subscribe,
      store.get,
   );

   const callReportApi = React.useCallback(
      async (call: ReportApiCall): Promise<ReportApiAnswer> => {
         try {
            const result = await app.callServerTool({
               name: "report_api",
               arguments: { ...call },
            });
            const answer = result.structuredContent as
               | ReportApiAnswer
               | undefined;
            if (answer && typeof answer.status === "number") return answer;
            return {
               status: 502,
               data: {
                  message:
                     textOf(result.content) ?? "No answer from Publisher.",
               },
            };
         } catch (e) {
            return {
               status: 502,
               data: { message: e instanceof Error ? e.message : String(e) },
            };
         }
      },
      [app],
   );

   const openLink = React.useCallback(
      (url: string) => {
         void app.openLink({ url }).catch(() => undefined);
      },
      [app],
   );

   return (
      <ReportPanel
         target={target}
         error={error}
         origin={origin}
         mode={mode}
         callReportApi={callReportApi}
         openLink={openLink}
      />
   );
}

async function start() {
   const app = new App({ name: "Malloy Publisher report", version: "1.0.0" });
   const store = panelStore(app);
   await app.connect();
   store.set({ mode: modeOf(app.getHostContext()?.theme) });
   ReactDOM.createRoot(document.getElementById("root")!).render(
      <Panel app={app} store={store} />,
   );
}

void start();
