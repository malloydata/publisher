import { FullConfig } from "@playwright/test";

/**
 * Wait for the publisher to reach `operationalState: "serving"` before
 * tests start. Playwright's `webServer.url` only checks that the endpoint
 * returns 2xx/3xx, but /api/v0/status returns 200 while still "initializing"
 * (packages downloading). We want specs to run only after packages are
 * fully loaded, so poll the state explicitly here.
 */
export default async function globalSetup(config: FullConfig): Promise<void> {
   const baseURL =
      config.projects[0]?.use?.baseURL ??
      process.env.PUBLISHER_URL ??
      "http://localhost:4000";
   const statusUrl = `${baseURL}/api/v0/status`;
   const deadline = Date.now() + 300_000; // 5 min ceiling
   let last: unknown = null;

   // eslint-disable-next-line no-console
   console.log(`[global-setup] polling ${statusUrl} for serving state...`);

   while (Date.now() < deadline) {
      try {
         const res = await fetch(statusUrl);
         if (res.ok) {
            const body = (await res.json()) as {
               operationalState?: string;
               initialized?: boolean;
               failedEnvironments?: Array<{ name: string; error: string }>;
            };
            last = body;
            if (body.operationalState === "serving") {
               // eslint-disable-next-line no-console
               console.log("[global-setup] server is serving — ready");
               return;
            }
            if (body.operationalState === "degraded") {
               // Fail fast: degraded means at least one configured
               // environment failed to init. Surface the failures so the
               // CI log explains *why* the suite didn't run instead of
               // making the developer poll the status endpoint.
               throw new Error(
                  `Publisher reached operationalState="degraded" — environment init failed. failedEnvironments: ${JSON.stringify(
                     body.failedEnvironments,
                  )}`,
               );
            }
         }
      } catch (err) {
         if (err instanceof Error && err.message.startsWith("Publisher")) {
            throw err;
         }
         // server not up yet
      }
      await new Promise((r) => setTimeout(r, 1000));
   }

   throw new Error(
      `Publisher did not reach operationalState="serving" within 5 min. Last status: ${JSON.stringify(last)}`,
   );
}
