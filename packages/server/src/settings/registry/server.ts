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
      aliases: { env: ["PUBLISHER_HOST"], flags: ["--host"] },
   }),
   setting({
      key: "rateLimit",
      type: integer({ min: 0 }),
      default: 0,
      description: "Requests per minute per client; 0 is unlimited.",
      aliases: { env: ["PUBLISHER_RATE_LIMIT"] },
   }),
   setting({
      key: "frameAncestors",
      type: string(),
      default: "'self'",
      description:
         "The Content-Security-Policy frame-ancestors source list: who may embed the app.",
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
