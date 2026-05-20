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
            };
            last = body;
            if (body.operationalState === "serving") {
               // eslint-disable-next-line no-console
               console.log("[global-setup] server is serving — ready");
               return;
            }
         }
      } catch {
         // server not up yet
      }
      await new Promise((r) => setTimeout(r, 1000));
   }

   throw new Error(
      `Publisher did not reach operationalState="serving" within 5 min. Last status: ${JSON.stringify(last)}`,
   );
}
