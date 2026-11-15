// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   AxiosError,
   AxiosHeaders,
   CanceledError,
   type AxiosAdapter,
   type AxiosResponse,
   type InternalAxiosRequestConfig,
} from "axios";

/**
 * The SDK's API calls, carried as `report_api` tool calls.
 *
 * Inside an MCP App panel there is no server to fetch from: the page runs in
 * the host's sandbox, on the host's origin, with no route to Publisher and no
 * credentials for it. Every request the SDK makes therefore goes back through
 * the host, which proxies a `report_api` call to the MCP server as the
 * connected user. This module turns the request the generated client built
 * (a method and a URL under `/api/v0`) into that call, and the call's answer
 * back into an axios response.
 *
 * Only the four reads `Report` needs are recognised; anything else is a 404
 * here, before it leaves the panel. The server enforces the same list.
 */

/** The arguments `report_api` takes (packages/server/src/mcp/tools/report_app_tools.ts). */
export interface ReportApiCall {
   operation: "status" | "listReports" | "getReport" | "query";
   environmentName?: string;
   packageName?: string;
   path?: string;
   versionId?: string;
   body?: Record<string, unknown>;
}

/** What `report_api` answers in its `structuredContent`. */
export interface ReportApiAnswer {
   status: number;
   data: unknown;
}

/**
 * Calls the tool through the host; `App.callServerTool` in the panel. The
 * signal is the request's own: aborting it cancels the tool call, so the
 * server stops the query and frees its slot.
 */
export type CallReportApi = (
   call: ReportApiCall,
   options?: { signal?: AbortSignal },
) => Promise<ReportApiAnswer>;

const decode = (segment: string) => decodeURIComponent(segment);

/**
 * The `report_api` call a request stands for, or undefined for one the panel
 * does not make. Exported for tests.
 */
export function toReportApiCall(
   method: string | undefined,
   url: string,
   data: unknown,
): ReportApiCall | undefined {
   const parsed = new URL(url, "http://panel.invalid/");
   // The generated client's paths start at the API root; strip it if the
   // base URL put it on.
   const pathname = parsed.pathname.replace(/^\/api\/v0(?=\/)/, "");
   const versionId = parsed.searchParams.get("versionId") ?? undefined;
   const segments = pathname.split("/").filter((s) => s.length > 0);
   const verb = (method ?? "get").toLowerCase();

   if (verb === "get" && segments.length === 1 && segments[0] === "status") {
      return { operation: "status" };
   }
   if (
      segments.length < 5 ||
      segments[0] !== "environments" ||
      segments[2] !== "packages"
   ) {
      return undefined;
   }
   const environmentName = decode(segments[1]);
   const packageName = decode(segments[3]);
   const rest = segments.slice(4);
   const scoped = {
      environmentName,
      packageName,
      ...(versionId ? { versionId } : {}),
   };

   if (verb === "get" && rest[0] === "reports" && rest.length === 1) {
      return { operation: "listReports", ...scoped };
   }
   if (verb === "get" && rest[0] === "reports" && rest.length > 1) {
      return {
         operation: "getReport",
         ...scoped,
         path: rest.slice(1).map(decode).join("/"),
      };
   }
   if (
      verb === "post" &&
      rest[0] === "models" &&
      rest.length > 2 &&
      rest[rest.length - 1] === "query"
   ) {
      return {
         operation: "query",
         ...scoped,
         path: rest.slice(1, -1).map(decode).join("/"),
         body: parseBody(data),
      };
   }
   return undefined;
}

function parseBody(data: unknown): Record<string, unknown> {
   if (typeof data === "string") {
      try {
         const parsed = JSON.parse(data);
         return parsed && typeof parsed === "object" ? parsed : {};
      } catch {
         return {};
      }
   }
   return data && typeof data === "object"
      ? (data as Record<string, unknown>)
      : {};
}

function respond(
   config: InternalAxiosRequestConfig,
   answer: ReportApiAnswer,
): AxiosResponse {
   return {
      data: answer.data,
      status: answer.status,
      statusText: answer.status < 400 ? "OK" : "Error",
      headers: new AxiosHeaders({ "content-type": "application/json" }),
      config,
   };
}

/**
 * An axios adapter for `ServerProvider`'s `adapter` prop that sends each
 * request through `callReportApi`, and settles it the way axios settles an
 * HTTP response: a status `validateStatus` refuses rejects with an
 * `AxiosError` carrying the response, so the SDK shows the server's message
 * as it would for a REST failure.
 */
export function reportApiAdapter(callReportApi: CallReportApi): AxiosAdapter {
   return async (config) => {
      // `useQueryResult` aborts a tile's request when its givens change or it
      // unmounts. axios refuses one already aborted before calling here, but
      // leaves an abort during the call to the adapter.
      const signal = config.signal as AbortSignal | undefined;
      const url = (config.baseURL ?? "") + (config.url ?? "");
      const call = toReportApiCall(config.method, url, config.data);
      const answer: ReportApiAnswer = call
         ? await callReportApi(call, { signal })
         : {
              status: 404,
              data: {
                 message: `The report panel does not make this request: ${(config.method ?? "get").toUpperCase()} ${config.url}`,
              },
           };
      if (signal?.aborted) throw new CanceledError(undefined, config);
      const response = respond(config, answer);
      const valid =
         config.validateStatus ?? ((s: number) => s >= 200 && s < 300);
      if (valid(response.status)) return response;
      const message =
         (answer.data as { message?: string } | undefined)?.message ??
         `Request failed with status code ${response.status}`;
      throw new AxiosError(
         message,
         response.status >= 500
            ? AxiosError.ERR_BAD_RESPONSE
            : AxiosError.ERR_BAD_REQUEST,
         config,
         undefined,
         response,
      );
   };
}
