// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The `snowflake` connection type's registry entry. */

import { createPrivateKey } from "crypto";
import type {
   ApiConnection,
   ConnectionDialect,
   CoreConnectionEntry,
   NamedConnection,
} from "./types";

export const SNOWFLAKE: ConnectionDialect = Object.freeze({
   type: "snowflake",
   attributes: Object.freeze({
      dialectName: "snowflake",
      isPool: true,
      canPersist: true,
      canStream: true,
   }),
   validateShape: validateSnowflakeShape,
   coreEntry: buildSnowflakeEntry,
});

/**
 * Collapse `null` to `undefined` for an optional connection field. The Malloy
 * config layer treats only `undefined` as absent: `makeDigest` reads `.length`
 * off every part and special-cases `undefined` alone, so a `null` throws.
 * `removeUndefined` does not help, since a `null` survives it.
 */
function nullToUndefined<T>(value: T | null | undefined): T | undefined {
   return value ?? undefined;
}

export function normalizeSnowflakePrivateKey(privateKey: string): string {
   let privateKeyContent = privateKey.trim();

   if (!privateKeyContent.includes("\n")) {
      const keyPatterns = [
         {
            beginRegex: /-----BEGIN\s+ENCRYPTED\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+ENCRYPTED\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN ENCRYPTED PRIVATE KEY-----",
            endMarker: "-----END ENCRYPTED PRIVATE KEY-----",
         },
         {
            beginRegex: /-----BEGIN\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN PRIVATE KEY-----",
            endMarker: "-----END PRIVATE KEY-----",
         },
         {
            beginRegex: /-----BEGIN\s+RSA\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+RSA\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN RSA PRIVATE KEY-----",
            endMarker: "-----END RSA PRIVATE KEY-----",
         },
      ];

      for (const pattern of keyPatterns) {
         const beginMatch = privateKeyContent.match(pattern.beginRegex);
         const endMatch = privateKeyContent.match(pattern.endRegex);

         if (beginMatch && endMatch) {
            const beginPos = beginMatch.index! + beginMatch[0].length;
            const endPos = endMatch.index!;
            const keyData = privateKeyContent
               .substring(beginPos, endPos)
               .replace(/\s+/g, "");

            const lines: string[] = [];
            for (let i = 0; i < keyData.length; i += 64) {
               lines.push(keyData.slice(i, i + 64));
            }
            privateKeyContent = `${pattern.beginMarker}\n${lines.join("\n")}\n${pattern.endMarker}\n`;
            break;
         }
      }
   } else if (!privateKeyContent.endsWith("\n")) {
      privateKeyContent += "\n";
   }

   // Snowflake's Node SDK requires PKCS#8 ("BEGIN PRIVATE KEY"). Convert
   // PKCS#1 ("BEGIN RSA PRIVATE KEY") so users can paste either format.
   if (/-----BEGIN\s+RSA\s+PRIVATE\s+KEY-----/i.test(privateKeyContent)) {
      try {
         privateKeyContent = createPrivateKey({
            key: privateKeyContent,
            format: "pem",
         })
            .export({ type: "pkcs8", format: "pem" })
            .toString();
      } catch (err) {
         throw new Error(
            `Failed to convert Snowflake RSA private key (PKCS#1) to PKCS#8: ${
               err instanceof Error ? err.message : String(err)
            }`,
         );
      }
      if (!privateKeyContent.endsWith("\n")) {
         privateKeyContent += "\n";
      }
   }

   return privateKeyContent;
}

function validateSnowflakeShape(connection: ApiConnection): void {
   const snowflakeConnection = connection.snowflakeConnection;
   if (!snowflakeConnection) {
      throw new Error("Snowflake connection configuration is missing.");
   }
   if (!snowflakeConnection.account) {
      throw new Error("Snowflake account is required.");
   }
   if (!snowflakeConnection.username) {
      throw new Error("Snowflake username is required.");
   }
   if (!snowflakeConnection.password && !snowflakeConnection.privateKey) {
      throw new Error(
         "Snowflake password or private key or private key path is required.",
      );
   }
   if (!snowflakeConnection.warehouse) {
      throw new Error("Snowflake warehouse is required.");
   }
}

function buildSnowflakeEntry(connection: NamedConnection): CoreConnectionEntry {
   return {
      is: "snowflake",
      account: connection.snowflakeConnection?.account,
      username: connection.snowflakeConnection?.username,
      password: nullToUndefined(connection.snowflakeConnection?.password),
      privateKey: connection.snowflakeConnection?.privateKey
         ? normalizeSnowflakePrivateKey(
              connection.snowflakeConnection.privateKey,
           )
         : undefined,
      privateKeyPass: nullToUndefined(
         connection.snowflakeConnection?.privateKeyPass,
      ),
      warehouse: connection.snowflakeConnection?.warehouse,
      // An explicit `null` here would throw in Malloy's `makeDigest` (see
      // nullToUndefined). Publisher builds every Snowflake connection itself
      // (snowflake_connection.ts), which drops them too; this entry only
      // registers the name.
      database: nullToUndefined(connection.snowflakeConnection?.database),
      schema: nullToUndefined(connection.snowflakeConnection?.schema),
      role: nullToUndefined(connection.snowflakeConnection?.role),
      timeoutMs: connection.snowflakeConnection?.responseTimeoutMilliseconds,
      // Pool sizing is server-owned policy, not exposed through the API.
      poolMin: 1,
      poolMax: 20,
   };
}
