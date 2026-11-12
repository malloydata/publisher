// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The two props a host that is not a browser tab on the server needs: an
// `adapter` that carries the API calls some other way (an MCP App panel sends
// them through its host), and a `mode` the host decides.
//
// Isolated (see test/README.md): it renders the real ServerProvider, which a
// spec that ran mockServerProvider earlier in the same `bun test` replaces.

import { afterEach, expect, it } from "bun:test";
import { cleanup, render, waitFor } from "@testing-library/react";
import type { AxiosAdapter } from "axios";
import { usePublisherTheme } from "../theme/ThemeContext";
import { ServerProvider, useServer } from "./ServerProvider";

afterEach(cleanup);

function Probe({ onReady }: { onReady: (v: unknown) => void }) {
   const { apiClients } = useServer();
   const { mode } = usePublisherTheme();
   return (
      <button
         data-mode={mode}
         onClick={async () =>
            onReady(
               (await apiClients.reports.getReport("e", "p", "a/b.malloy"))
                  .data,
            )
         }
      >
         go
      </button>
   );
}

it("sends every API call through the adapter instead of HTTP", async () => {
   const seen: { method?: string; url?: string }[] = [];
   const adapter: AxiosAdapter = async (config) => {
      seen.push({ method: config.method, url: config.url });
      return {
         data: config.url?.endsWith("/status")
            ? { theme: {} }
            : { title: "From the adapter" },
         status: 200,
         statusText: "OK",
         headers: {},
         config,
      };
   };
   let answer: unknown;
   const { getByText } = render(
      <ServerProvider baseURL="http://host.invalid/api/v0" adapter={adapter}>
         <Probe onReady={(v) => (answer = v)} />
      </ServerProvider>,
   );
   getByText("go").click();
   await waitFor(() => expect(answer).toEqual({ title: "From the adapter" }));
   expect(seen).toContainEqual({
      method: "get",
      url: "/environments/e/packages/p/reports/a%2Fb.malloy",
   });
});

it("a host's mode wins over the viewer's stored choice and the OS", async () => {
   try {
      window.localStorage.setItem("publisher:themeMode", "light");
   } catch {
      // A DOM without storage: the prop alone is under test then.
   }
   const adapter: AxiosAdapter = async (config) => ({
      data: { theme: { defaultMode: "light" } },
      status: 200,
      statusText: "OK",
      headers: {},
      config,
   });
   const { getByText } = render(
      <ServerProvider
         baseURL="http://mode.invalid/api/v0"
         adapter={adapter}
         mode="dark"
      >
         <Probe onReady={() => undefined} />
      </ServerProvider>,
   );
   await waitFor(() =>
      expect(getByText("go").getAttribute("data-mode")).toBe("dark"),
   );
   try {
      window.localStorage.removeItem("publisher:themeMode");
   } catch {
      // see above
   }
});
