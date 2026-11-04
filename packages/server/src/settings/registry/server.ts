// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DEFAULT_CALLER_HEADER } from "../../constants";
import { setting } from "../setting";
import { boolean, integer, oneOf, string, structured } from "../types";

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
      key: "callerHeader",
      type: string(),
      default: DEFAULT_CALLER_HEADER,
      description:
         "Request header the access record copies as the caller: the identity the gateway in front of Publisher asserts.",
      notes: "The request header whose value the per-query access record copies as `caller`: the identity the gateway in front of Publisher asserts. Opaque to Publisher; see [access-record.md](access-record.md).",
      aliases: { env: ["PUBLISHER_CALLER_HEADER"] },
   }),
   setting({
      key: "packageVersioning",
      type: oneOf(["off", "on"]),
      default: "off",
      description:
         "on publishes a package from a location as the version its publisher.json declares; off installs it in place. A transition setting #1381 removes.",
      notes: "`on` or `off`: whether a publish from a location publishes an immutable [package version](package-versions.md) numbered by its `publisher.json` `version` (a missing or non-semver `version` is then a 400), or installs the package in place as before versions. A transition setting: a future release removes it and requires a `version` ([#1381](https://github.com/malloydata/publisher/issues/1381)). Versions published while it was on keep serving when it is off. Any other value fails startup.",
      aliases: { env: ["PUBLISHER_PACKAGE_VERSIONING"] },
   }),
   setting({
      key: "versionPromotion",
      type: oneOf(["on-publish", "explicit"]),
      default: "on-publish",
      description:
         "on-publish moves a package's latest to a newly published higher version; explicit leaves it to PUT …/latest.",
      notes: "`on-publish` or `explicit`: what publishing a package version does to its `latest`. `on-publish` moves `latest` to the new version unless a higher one already is; `explicit` never moves it, so only `PUT .../packages/{package}/latest` does, for an orchestrator that decides when a version is ready. Also settable as `versionPromotion` in `publisher.config.json`; the variable wins. Any other value fails startup. See [package-versions.md](package-versions.md).",
      aliases: {
         env: ["PUBLISHER_VERSION_PROMOTION"],
         file: ["versionPromotion"],
      },
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
