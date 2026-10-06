// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { canRetryRequest } from "./canRetry";

const axiosError = (status: number) =>
   Object.assign(new Error(`Request failed with status code ${status}`), {
      response: { status },
   });

describe("canRetryRequest", () => {
   it("refuses a retry that cannot change the answer: 401, 403, 404", () => {
      for (const status of [401, 403, 404]) {
         expect(canRetryRequest(axiosError(status))).toBe(false);
         expect(canRetryRequest({ status })).toBe(false);
      }
   });

   it("allows one for any other status", () => {
      for (const status of [400, 408, 409, 429, 500, 502, 503])
         expect(canRetryRequest(axiosError(status))).toBe(true);
   });

   it("allows one when there was no response to read", () => {
      expect(canRetryRequest(new Error("Network Error"))).toBe(true);
      expect(canRetryRequest(undefined)).toBe(true);
      expect(canRetryRequest(null)).toBe(true);
      expect(canRetryRequest("timeout")).toBe(true);
      expect(canRetryRequest({ response: {} })).toBe(true);
   });
});
