// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** SHA-256 of a string, hex — the `expectedHash` a dashboard write carries. */
export async function sha256Hex(text: string): Promise<string> {
   const digest = await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(text),
   );
   return Array.from(new Uint8Array(digest), (byte) =>
      byte.toString(16).padStart(2, "0"),
   ).join("");
}
