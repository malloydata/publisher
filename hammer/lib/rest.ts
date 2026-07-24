// Thin REST client for the running Publisher server. One instance is bound to a
// (baseUrl, environment) pair. Everything the scenarios need: package inspection,
// materialization create/poll, manifest binding, and querying with both the
// compiled-SQL (routing evidence) and the row values (correctness).

export type MaterializationStatus =
   | "PENDING"
   | "COMPILING"
   | "BUILDING"
   | "MANIFEST_FILE_READY"
   | "FAILED"
   | "CANCELLED"
   | string;

const TERMINAL = new Set(["MANIFEST_FILE_READY", "FAILED", "CANCELLED"]);

export interface QueryOutcome {
   /** The SQL the served query compiled to — routing evidence. */
   sql: string;
   /** Row values (from the compact result form). */
   rows: Record<string, unknown>[];
   /** Raw full-result JSON string, for anything else a scenario wants. */
   raw: string;
}

export class Rest {
   constructor(
      readonly baseUrl: string,
      readonly env: string,
   ) {}

   private pkgUrl(pkg: string, suffix = ""): string {
      return `${this.baseUrl}/api/v0/environments/${this.env}/packages/${pkg}${suffix}`;
   }

   async status(): Promise<{ operationalState?: string; loadErrors?: unknown }> {
      const res = await fetch(`${this.baseUrl}/api/v0/status`);
      return (await res.json()) as { operationalState?: string; loadErrors?: unknown };
   }

   /**
    * (Re-)publish a package through the author-in-the-loop gate
    * (POST /environments/:env/packages with `{ name }`, no location — registering
    * the already-on-disk directory). Unlike startup/reload (fail-safe, warn-only),
    * this path is strict, so it 4xx's on e.g. a persist-target collision under
    * PERSIST_COLLISION_ENFORCE. Returns the outcome rather than throwing so a
    * scenario can assert either an accept or a refusal.
    */
   async addPackage(
      pkg: string,
   ): Promise<{ ok: true } | { ok: false; status: number; error: string }> {
      const res = await fetch(
         `${this.baseUrl}/api/v0/environments/${this.env}/packages`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name: pkg }),
         },
      );
      if (res.ok) return { ok: true };
      return { ok: false, status: res.status, error: await res.text() };
   }

   async listConnections(): Promise<{ name: string }[]> {
      const res = await fetch(
         `${this.baseUrl}/api/v0/environments/${this.env}/connections`,
      );
      if (!res.ok) throw new Error(`listConnections ${res.status}: ${await res.text()}`);
      return (await res.json()) as { name: string }[];
   }

   async getPackage(
      pkg: string,
      opts: { reload?: boolean } = {},
   ): Promise<Record<string, unknown>> {
      const res = await fetch(this.pkgUrl(pkg, opts.reload ? "?reload=true" : ""));
      if (!res.ok) throw new Error(`getPackage ${pkg} ${res.status}: ${await res.text()}`);
      return (await res.json()) as Record<string, unknown>;
   }

   /**
    * Compile-check a model on demand (POST /compile). `source` is appended to the
    * target model for namespace context (pass "" to compile the model as-is).
    * Returns structured diagnostics WITHOUT loading the package into the serving
    * set — the deterministic way to assert a model does/doesn't compile.
    */
   async compile(
      pkg: string,
      modelPath: string,
      source: string,
   ): Promise<{ status: string; problems: { severity?: string; message?: string }[] }> {
      const res = await fetch(this.pkgUrl(pkg, `/models/${modelPath}/compile`), {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({ source }),
      });
      const body = (await res.json()) as {
         status?: string;
         problems?: { severity?: string; message?: string }[];
         code?: number;
         message?: string;
      };
      // A compile FAILURE surfaces two ways: 200 with {status:"error", problems}
      // when Malloy returns diagnostics, OR 424 (ModelCompilationError) with a
      // {message} when the compile throws. Normalize both to a compile-error
      // result; only a genuinely unexpected status is a harness error.
      if (res.ok) {
         return {
            status: body.status ?? "success",
            problems: body.problems ?? [],
         };
      }
      if (res.status === 424) {
         return { status: "error", problems: [{ message: body.message ?? "" }] };
      }
      throw new Error(`compile ${pkg}/${modelPath} ${res.status}: ${JSON.stringify(body)}`);
   }

   /** Map sourceName -> sourceEntityId from the package build plan. */
   async sourceEntityIds(pkg: string): Promise<Record<string, string>> {
      const p = (await this.getPackage(pkg)) as {
         buildPlan?: { sources?: Record<string, { sourceEntityId?: string }> };
      };
      const out: Record<string, string> = {};
      // Keys are sourceIDs ("sourceName@modelURL"); index by the bare source name.
      for (const [sourceID, s] of Object.entries(p.buildPlan?.sources ?? {})) {
         if (s.sourceEntityId) out[sourceID.split("@")[0]] = s.sourceEntityId;
      }
      return out;
   }

   /** Unload + delete a package from the serving set (DELETE /packages/:pkg). */
   async deletePackage(pkg: string): Promise<void> {
      const res = await fetch(this.pkgUrl(pkg), { method: "DELETE" });
      if (!res.ok)
         throw new Error(`deletePackage ${pkg} ${res.status}: ${await res.text()}`);
   }

   async patchPackage(
      pkg: string,
      body: Record<string, unknown>,
   ): Promise<Record<string, unknown>> {
      const res = await fetch(this.pkgUrl(pkg), {
         method: "PATCH",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({ name: pkg, ...body }),
      });
      const json = (await res.json()) as Record<string, unknown>;
      if (!res.ok)
         throw new Error(`patchPackage ${pkg} ${res.status}: ${JSON.stringify(json)}`);
      return json;
   }

   /**
    * The `entries` map of the package's most recent successful materialization
    * (MANIFEST_FILE_READY). This is the manifest the orchestrator would stamp
    * and distribute; the harness re-serves it via `writeManifest` + a
    * `manifestLocation` PATCH to exercise the orchestrator bind path.
    */
   async latestManifestEntries(pkg: string): Promise<Record<string, unknown>> {
      const runs = await this.listMaterializations(pkg);
      const latest = runs.find(
         (m) =>
            (m.status as string) === "MANIFEST_FILE_READY" &&
            (m.manifest as { entries?: unknown } | undefined)?.entries,
      );
      const entries = (latest?.manifest as { entries?: Record<string, unknown> } | undefined)?.entries;
      return entries ?? {};
   }

   /** Kick off a materialization. Empty body = auto-run; pass buildInstructions for orchestrated. */
   async createMaterialization(
      pkg: string,
      body: Record<string, unknown> = {},
   ): Promise<{ id: string; status: MaterializationStatus }> {
      const res = await fetch(this.pkgUrl(pkg, "/materializations"), {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify(body),
      });
      const json = (await res.json()) as { id: string; status: MaterializationStatus };
      if (!res.ok)
         throw new Error(
            `createMaterialization ${pkg} ${res.status}: ${JSON.stringify(json)}`,
         );
      return json;
   }

   async getMaterialization(
      pkg: string,
      id: string,
   ): Promise<Record<string, unknown>> {
      const res = await fetch(this.pkgUrl(pkg, `/materializations/${id}`));
      if (!res.ok)
         throw new Error(`getMaterialization ${res.status}: ${await res.text()}`);
      return (await res.json()) as Record<string, unknown>;
   }

   async pollMaterialization(
      pkg: string,
      id: string,
      timeoutMs = 120_000,
   ): Promise<Record<string, unknown>> {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
         const rec = await this.getMaterialization(pkg, id);
         if (TERMINAL.has(rec.status as string)) return rec;
         await new Promise((r) => setTimeout(r, 300));
      }
      throw new Error(`materialization ${id} did not reach a terminal state`);
   }

   /** Build + wait; throws with the failure reason unless it reached MANIFEST_FILE_READY. */
   async build(
      pkg: string,
      body: Record<string, unknown> = {},
   ): Promise<Record<string, unknown>> {
      const { id } = await this.createMaterialization(pkg, body);
      const rec = await this.pollMaterialization(pkg, id);
      if (rec.status !== "MANIFEST_FILE_READY") {
         throw new Error(
            `build ${pkg} ended ${rec.status}: ${rec.error ?? rec.message ?? "(no reason)"}`,
         );
      }
      return rec;
   }

   async deleteMaterialization(
      pkg: string,
      id: string,
      opts: { dropTables?: boolean } = {},
   ): Promise<void> {
      const suffix = `/materializations/${id}${opts.dropTables ? "?dropTables=true" : ""}`;
      const res = await fetch(this.pkgUrl(pkg, suffix), { method: "DELETE" });
      if (!res.ok)
         throw new Error(
            `deleteMaterialization ${pkg}/${id} ${res.status}: ${await res.text()}`,
         );
   }

   /**
    * Reclaim the most recent successful materialization: DELETE it with
    * `?dropTables=true`, which runs the destination-aware read-write drop of the
    * physical table. Returns the id reclaimed (throws if there is none).
    */
   async reclaimLatest(pkg: string): Promise<string> {
      const mats = await this.listMaterializations(pkg);
      const ready = mats
         .filter((m) => m.status === "MANIFEST_FILE_READY")
         .sort((a, b) => String(b.id).localeCompare(String(a.id)));
      const id = ready[0]?.id as string | undefined;
      if (!id) throw new Error(`reclaimLatest ${pkg}: no successful materialization`);
      await this.deleteMaterialization(pkg, id, { dropTables: true });
      return id;
   }

   async listMaterializations(pkg: string): Promise<Record<string, unknown>[]> {
      const res = await fetch(this.pkgUrl(pkg, "/materializations"));
      if (!res.ok) throw new Error(`listMaterializations ${res.status}`);
      const json = (await res.json()) as unknown;
      return (Array.isArray(json) ? json : ((json as { materializations?: [] }).materializations ?? [])) as Record<
         string,
         unknown
      >[];
   }

   /** Run a raw SQL statement against a registered connection (e.g. operator DDL). */
   async connectionSql(conn: string, sqlStatement: string): Promise<unknown> {
      const res = await fetch(
         `${this.baseUrl}/api/v0/environments/${this.env}/connections/${conn}/sqlQuery`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ sqlStatement }),
         },
      );
      const json = (await res.json()) as unknown;
      if (!res.ok)
         throw new Error(`connectionSql ${conn} ${res.status}: ${JSON.stringify(json)}`);
      return json;
   }

   /** Query a model, returning both the compiled SQL and the row values. */
   async query(
      pkg: string,
      modelPath: string,
      spec: {
         query?: string;
         sourceName?: string;
         queryName?: string;
         givens?: Record<string, unknown>;
      },
   ): Promise<QueryOutcome> {
      const url = this.pkgUrl(pkg, `/models/${modelPath}/query`);
      const post = async (compactJson: boolean): Promise<string> => {
         const res = await fetch(url, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ ...spec, compactJson }),
         });
         const json = (await res.json()) as { result?: string; error?: string };
         if (!res.ok)
            throw new Error(
               `query ${pkg}/${modelPath} ${res.status}: ${json.error ?? JSON.stringify(json)}`,
            );
         return json.result ?? "";
      };
      const full = await post(false);
      const compact = await post(true);
      const sql = (JSON.parse(full) as { sql?: string }).sql ?? "";
      let rows: Record<string, unknown>[] = [];
      try {
         const parsed = JSON.parse(compact);
         rows = Array.isArray(parsed) ? parsed : [];
      } catch {
         rows = [];
      }
      return { sql, rows, raw: full };
   }

   /** Query, tolerating failure — returns the error string instead of throwing. */
   async tryQuery(
      pkg: string,
      modelPath: string,
      spec: {
         query?: string;
         sourceName?: string;
         queryName?: string;
         givens?: Record<string, unknown>;
      },
   ): Promise<{ ok: true; outcome: QueryOutcome } | { ok: false; error: string }> {
      try {
         return { ok: true, outcome: await this.query(pkg, modelPath, spec) };
      } catch (e) {
         return { ok: false, error: (e as Error).message };
      }
   }
}
