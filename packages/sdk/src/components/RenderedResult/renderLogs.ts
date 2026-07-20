import { LogMessage } from "../../client";

export interface RenderLogSummary {
   severity: "warn" | "error";
   title: string;
}

// Severity describes the tag defect, not whether the chart drew, so findings are
// only ever annotated, never used to withhold a result.
const SHOWN_SEVERITIES = new Set(["warn", "error"]);

// Every LogMessage field is optional, so entries without a severity we show or
// without a message are dropped rather than rendered as an empty tooltip.
export function summarizeRenderLogs(
   logs: LogMessage[] | undefined,
): RenderLogSummary | undefined {
   if (!logs?.length) {
      return undefined;
   }
   const messages: string[] = [];
   let hasError = false;
   for (const log of logs) {
      if (!log.message || !SHOWN_SEVERITIES.has(log.severity ?? "")) {
         continue;
      }
      if (log.severity === "error") {
         hasError = true;
      }
      if (!messages.includes(log.message)) {
         messages.push(log.message);
      }
   }
   if (!messages.length) {
      return undefined;
   }
   return {
      severity: hasError ? "error" : "warn",
      title: messages.join("\n"),
   };
}
