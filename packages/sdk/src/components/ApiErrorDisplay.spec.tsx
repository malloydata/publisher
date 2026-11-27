// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "bun:test";
import { type ApiError, ApiErrorDisplay } from "./ApiErrorDisplay";

// `useQueryWithApiError` fills `data` only when axios got a response body.
// Every other failure reaches the display as the raw error, and used to read
// "Unknown error" with its message and status thrown away.

const shown = (error: ApiError) => {
   render(<ApiErrorDisplay error={error} context="env > pkg > ops" />);
   return screen.getByText("env > pkg > ops").nextElementSibling?.textContent;
};

describe("ApiErrorDisplay", () => {
   it("shows the server's message when the server sent one", () => {
      const error: ApiError = Object.assign(new Error("ignored"), {
         status: 400,
         data: { code: 400, message: "filter 'state' is required" },
      });
      expect(shown(error)).toBe("filter 'state' is required");
   });

   it("shows the client's message when no response came back", () => {
      // axios's wording for a request that never got a response.
      expect(shown(new Error("Network Error"))).toBe("Network Error");
   });

   it("adds the status to a message that does not carry it", () => {
      const error: ApiError = Object.assign(new Error("Bad Gateway"), {
         status: 502,
      });
      expect(shown(error)).toBe("Bad Gateway (HTTP 502)");
   });

   it("does not repeat a status axios already put in its message", () => {
      const error: ApiError = Object.assign(
         new Error("Request failed with status code 502"),
         { status: 502 },
      );
      expect(shown(error)).toBe("Request failed with status code 502");
   });

   it("reads the status off the response when the error has none", () => {
      const error = Object.assign(new Error("Bad Gateway"), {
         response: { status: 502 },
      }) as ApiError;
      expect(shown(error)).toBe("Bad Gateway (HTTP 502)");
   });

   it("shows a status with no message", () => {
      const error: ApiError = Object.assign(new Error(""), { status: 503 });
      expect(shown(error)).toBe("The request failed with HTTP 503.");
   });

   it("says what it lacks when the error carries nothing", () => {
      expect(shown(new Error(""))).toBe(
         "The request failed, and the error carried no message or status.",
      );
   });
});
