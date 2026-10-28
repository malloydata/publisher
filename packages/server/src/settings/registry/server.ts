// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { setting } from "../setting";
import { boolean, integer, string, structured } from "../types";

/** The configuration design's "Server (top level)" table. */
export const server = [
   setting({
      key: "frozenConfig",
      type: boolean(),
      default: false,
      description:
         "Refuse changes to environments, packages and the theme through the API.",
      aliases: { file: ["frozenConfig"], today: { file: "truthy" } },
   }),
   setting({
      key: "servingApiEnabled",
      type: boolean(),
      default: true,
      description:
         "Run the main API and MCP, and the environments they serve; off runs only the admin API.",
      notes: "The top-level `servingApiEnabled` setting ([below](#server-settings-top-level-of-publisherconfigjson)); overrides the config file.",
      aliases: {
         env: ["PUBLISHER_SERVING_API_ENABLED"],
         flags: ["--serving-api-enabled", "--no-serving-api-enabled"],
         file: ["servingApiEnabled"],
         today: { file: "boolean-or-reference" },
      },
   }),
   setting({
      key: "adminApiEnabled",
      type: boolean(),
      default: true,
      description: "Allow the admin API; off refuses an admin setting.",
      notes: "The top-level `adminApiEnabled` setting ([below](#server-settings-top-level-of-publisherconfigjson)); overrides the config file.",
      aliases: {
         env: ["PUBLISHER_ADMIN_API_ENABLED"],
         flags: ["--admin-api-enabled", "--no-admin-api-enabled"],
         file: ["adminApiEnabled"],
         today: { file: "boolean-or-reference" },
      },
   }),
   setting({
      key: "port",
      type: integer({ min: 0, max: 65535 }),
      default: 4000,
      description: "Port of the REST API and the web app.",
      notes: "REST + static-app HTTP port.",
      aliases: {
         env: ["PUBLISHER_PORT"],
         flags: ["--port"],
      },
   }),
   setting({
      key: "host",
      type: string(),
      default: "0.0.0.0",
      description:
         "Host the REST API binds to, and MCP too unless mcp.host is given.",
      notes: "Host binding for the REST server, and the fallback for the MCP server. Set `127.0.0.1` to keep both loopback-only.",
      aliases: { env: ["PUBLISHER_HOST"], flags: ["--host"] },
   }),
   setting({
      key: "rateLimit",
      type: integer({ min: 0 }),
      default: 0,
      description: "Requests per minute per client; 0 is unlimited.",
      notes: "Maximum requests per minute one client may make to the REST server; over it, requests get `429` with `RateLimit-*` headers until the minute rolls over. Unset or `0` means no limit; any other value must be a non-negative integer or startup fails. `/health*` and `/metrics` are never limited. Clients are told apart by the connection's peer address, so behind a reverse proxy every client shares one bucket: rate-limit at the proxy there, or leave this unset. The MCP port is not covered.",
      defaultText: "`0` (no limit)",
      aliases: { env: ["PUBLISHER_RATE_LIMIT"] },
   }),
   setting({
      key: "frameAncestors",
      type: string(),
      default: "'self'",
      description:
         "The Content-Security-Policy frame-ancestors source list: who may embed the app.",
      notes: "`Content-Security-Policy: frame-ancestors` value sent on **every** document this server returns -- the Console and in-package `public/` files alike -- controlling which origins may embed a page in an iframe. Defaults to same-origin only; set it to the origins that embed you (`https://app.example.com`), or to `*` to restore the previous any-origin behaviour.",
      aliases: { env: ["PUBLISHER_FRAME_ANCESTORS"] },
   }),
   setting({
      key: "theme",
      type: structured(),
      kind: "structured",
      description: "The instance-wide default theme of the app.",
      aliases: { file: ["theme"] },
   }),
   setting({
      key: "environments",
      type: structured(),
      kind: "structured",
      description:
         "The environments to serve, with their packages and connections.",
      aliases: { file: ["environments", "projects"] },
   }),
];
