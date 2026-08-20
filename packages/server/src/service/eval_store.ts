import { createHash } from "crypto";
import fs from "fs";
import path from "path";
import { BadRequestError, ConflictError, NotFoundError } from "../errors";
import {
   assertSafeRelativeModelPath,
   safeJoinUnderRoot,
} from "../path_safety";
import type { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";

export type EvalSetStatus = "active" | "archived";
export type EvalCaseState = "candidate" | "selected" | "excluded";
export type EvalGoldenStatus =
   | "missing"
   | "provisional"
   | "verified"
   | "invalid"
   | "ambiguous";
export type EvalEventKind =
   | "attempt"
   | "tool_call"
   | "score"
   | "issue"
   | "issue_status"
   | "candidate"
   | "gate"
   | "checkpoint";

export interface EvalSet {
   id: string;
   name: string;
   description?: string;
   targetModelPath?: string;
   environmentName?: string;
   packageName?: string;
   draftRevision: number;
   exportRevision?: number;
   metadata?: Record<string, unknown>;
   status: EvalSetStatus;
   createdAt: string;
   updatedAt: string;
}

export interface EvalCase {
   id: string;
   setId: string;
   qid: string;
   question: string;
   split?: string;
   tags?: string[];
   state: EvalCaseState;
   source?: Record<string, unknown>;
   selection?: Record<string, unknown>;
   golden?: Record<string, unknown>;
   goldenRevision: number;
   artifactRefs?: Record<string, unknown>;
   createdAt: string;
   updatedAt: string;
}

export interface EvalEvidence {
   id: string;
   setId: string;
   caseId?: string;
   kind: string;
   snippet?: string;
   localRef?: string;
   sensitivity?: string;
   redaction?: Record<string, unknown>;
   createdAt: string;
}

export interface EvalRun {
   id: string;
   setId: string;
   setRevision: number;
   config: Record<string, unknown>;
   status: string;
   summary?: Record<string, unknown>;
   createdAt: string;
   updatedAt: string;
}

export interface EvalEvent {
   id: string;
   runId: string;
   caseId?: string;
   kind: EvalEventKind;
   payload: Record<string, unknown>;
   createdAt: string;
}

export interface EvalCheckpointFile {
   path: string;
   content: string;
   sha256: string;
}

export interface EvalCheckpoint {
   id: string;
   label: string;
   runId?: string;
   environmentName: string;
   packageName: string;
   modelPath: string;
   servedRevision?: string;
   sourceContentSha?: string;
   issueIds: string[];
   files: EvalCheckpointFile[];
   createdAt: string;
}

const MAX_CHECKPOINT_FILES = 32;
const MAX_CHECKPOINT_FILE_CHARS = 2_000_000;

function sha256Text(content: string): string {
   return createHash("sha256").update(content).digest("hex");
}

function normalizeCheckpointFiles(
   files: Array<{ path: string; content: string; sha256?: string }>,
): EvalCheckpointFile[] {
   if (!Array.isArray(files) || files.length === 0) {
      throw new BadRequestError("A checkpoint needs at least one model file");
   }
   if (files.length > MAX_CHECKPOINT_FILES) {
      throw new BadRequestError(
         `A checkpoint may snapshot at most ${MAX_CHECKPOINT_FILES} files`,
      );
   }
   return files.map((file) => {
      assertSafeRelativeModelPath(file.path);
      if (typeof file.content !== "string") {
         throw new BadRequestError(`Checkpoint file ${file.path} needs text content`);
      }
      if (file.content.length > MAX_CHECKPOINT_FILE_CHARS) {
         throw new BadRequestError(
            `Checkpoint file ${file.path} exceeds ${MAX_CHECKPOINT_FILE_CHARS} characters`,
         );
      }
      const sha256 = sha256Text(file.content);
      if (file.sha256 && file.sha256 !== sha256) {
         throw new BadRequestError(
            `Checkpoint file ${file.path} sha256 does not match content`,
         );
      }
      return { path: file.path, content: file.content, sha256 };
   });
}

function withoutFileContent(checkpoint: EvalCheckpoint): EvalCheckpoint {
   return {
      ...checkpoint,
      files: checkpoint.files.map((file) => ({
         path: file.path,
         sha256: file.sha256,
         content: "",
      })),
   };
}

export interface EvalSnapshot {
   eval: {
      name: string;
      description?: string;
      version: number;
      targetModelPath?: string;
      environmentName?: string;
      packageName?: string;
      casesPath: string;
      splits?: string[];
      notes?: string;
   };
   cases: Array<Record<string, unknown>>;
}

function nowIso(): string {
   return new Date().toISOString();
}

/** DuckDB prepared UPDATEs reject JS null as type ANY. Always bind JSON text. */
function jsonCol(value: unknown): string {
   return JSON.stringify(value ?? null);
}

/** REST handlers pass omitted fields as `undefined`; spreading those would wipe the row. */
function definedPatch<T extends Record<string, unknown>>(patch: T): Partial<T> {
   return Object.fromEntries(
      Object.entries(patch).filter(([, value]) => value !== undefined),
   ) as Partial<T>;
}

function parseJson<T>(raw: string | null | undefined, fallback: T): T {
   if (!raw) return fallback;
   try {
      return JSON.parse(raw) as T;
   } catch {
      return fallback;
   }
}

const SAFE_SET_NAME_RE = /^(?!\.)[A-Za-z0-9._-]{1,128}$/;

function assertSafeSetName(name: unknown): asserts name is string {
   if (typeof name !== "string" || !SAFE_SET_NAME_RE.test(name)) {
      throw new BadRequestError(
         `Invalid eval set name: must be 1-128 characters of letters, digits, "-", "_", or "."`,
      );
   }
}

function parseSetStatus(raw: unknown): EvalSetStatus {
   return raw === "archived" ? "archived" : "active";
}

export class EvalStore {
   constructor(private db: DuckDBConnection) {}

   async listSets(filter?: {
      status?: EvalSetStatus | "all";
      name?: string;
   }): Promise<EvalSet[]> {
      const status = filter?.status ?? "active";
      const name = filter?.name;
      const rows = await this.db.all<Record<string, unknown>>(
         `SELECT * FROM eval_sets ORDER BY updated_at DESC`,
      );
      return rows
         .map((row) => this.toSet(row))
         .filter((set) => (name ? set.name === name : true))
         .filter((set) => (status === "all" ? true : set.status === status));
   }

   async getSet(id: string): Promise<EvalSet> {
      const row = await this.db.get<Record<string, unknown>>(
         `SELECT * FROM eval_sets WHERE id = ?`,
         [id],
      );
      if (!row) throw new NotFoundError(`Eval set not found: ${id}`);
      return this.toSet(row);
   }

   async createSet(input: {
      name: string;
      description?: string;
      targetModelPath?: string;
      environmentName?: string;
      packageName?: string;
      metadata?: Record<string, unknown>;
   }): Promise<EvalSet> {
      assertSafeSetName(input.name);
      await this.assertNoActiveName(input.name);
      const now = nowIso();
      const set: EvalSet = {
         id: crypto.randomUUID(),
         name: input.name,
         description: input.description,
         targetModelPath: input.targetModelPath,
         environmentName: input.environmentName,
         packageName: input.packageName,
         draftRevision: 1,
         metadata: input.metadata,
         status: "active",
         createdAt: now,
         updatedAt: now,
      };
      await this.db.run(
         `INSERT INTO eval_sets (
            id, name, description, target_model_path, environment_name,
            package_name, draft_revision, export_revision, metadata_json,
            status, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)`,
         [
            set.id,
            set.name,
            set.description ?? null,
            set.targetModelPath ?? null,
            set.environmentName ?? null,
            set.packageName ?? null,
            set.draftRevision,
            set.metadata ? JSON.stringify(set.metadata) : null,
            set.status,
            set.createdAt,
            set.updatedAt,
         ],
      );
      return set;
   }

   async updateSet(
      id: string,
      patch: Partial<
         Pick<
            EvalSet,
            | "name"
            | "description"
            | "targetModelPath"
            | "environmentName"
            | "packageName"
            | "metadata"
            | "status"
         >
      >,
   ): Promise<EvalSet> {
      const current = await this.getSet(id);
      const fields = definedPatch(patch);
      if (fields.name !== undefined) assertSafeSetName(fields.name);
      const next: EvalSet = {
         ...current,
         ...fields,
         draftRevision: current.draftRevision + 1,
         updatedAt: nowIso(),
      };
      if (next.status === "active") {
         await this.assertNoActiveName(next.name, id);
      }
      await this.db.run(
         `UPDATE eval_sets SET
            name = CAST(? AS VARCHAR),
            description = CAST(? AS VARCHAR),
            target_model_path = CAST(? AS VARCHAR),
            environment_name = CAST(? AS VARCHAR),
            package_name = CAST(? AS VARCHAR),
            draft_revision = CAST(? AS INTEGER),
            metadata_json = CAST(? AS VARCHAR),
            status = CAST(? AS VARCHAR),
            updated_at = CAST(? AS TIMESTAMP)
          WHERE id = CAST(? AS VARCHAR)`,
         [
            next.name,
            next.description ?? null,
            next.targetModelPath ?? null,
            next.environmentName ?? null,
            next.packageName ?? null,
            next.draftRevision,
            jsonCol(next.metadata),
            next.status,
            next.updatedAt,
            id,
         ],
      );
      return next;
   }

   async listCases(setId: string): Promise<EvalCase[]> {
      await this.getSet(setId);
      const rows = await this.db.all<Record<string, unknown>>(
         `SELECT * FROM eval_cases WHERE set_id = ? ORDER BY qid`,
         [setId],
      );
      return rows.map((row) => this.toCase(row));
   }

   async getCase(id: string): Promise<EvalCase> {
      const row = await this.db.get<Record<string, unknown>>(
         `SELECT * FROM eval_cases WHERE id = ?`,
         [id],
      );
      if (!row) throw new NotFoundError(`Eval case not found: ${id}`);
      return this.toCase(row);
   }

   async createCase(
      setId: string,
      input: {
         qid: string;
         question: string;
         split?: string;
         tags?: string[];
         state?: EvalCaseState;
         source?: Record<string, unknown>;
         selection?: Record<string, unknown>;
         golden?: Record<string, unknown>;
         artifactRefs?: Record<string, unknown>;
      },
   ): Promise<EvalCase> {
      await this.getSet(setId);
      if (!input.qid?.trim() || !input.question?.trim()) {
         throw new BadRequestError("qid and question are required");
      }
      const now = nowIso();
      const record: EvalCase = {
         id: crypto.randomUUID(),
         setId,
         qid: input.qid,
         question: input.question,
         split: input.split,
         tags: input.tags,
         state: input.state ?? "candidate",
         source: input.source,
         selection: input.selection,
         golden: input.golden ?? { status: "missing" },
         goldenRevision: input.golden ? 1 : 0,
         artifactRefs: input.artifactRefs,
         createdAt: now,
         updatedAt: now,
      };
      await this.db.run(
         `INSERT INTO eval_cases (
            id, set_id, qid, question, split, tags_json, state, source_json,
            selection_json, golden_json, golden_revision, artifact_refs_json,
            created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
         [
            record.id,
            record.setId,
            record.qid,
            record.question,
            record.split ?? null,
            record.tags ? JSON.stringify(record.tags) : null,
            record.state,
            record.source ? JSON.stringify(record.source) : null,
            record.selection ? JSON.stringify(record.selection) : null,
            record.golden ? JSON.stringify(record.golden) : null,
            record.goldenRevision,
            record.artifactRefs ? JSON.stringify(record.artifactRefs) : null,
            record.createdAt,
            record.updatedAt,
         ],
      );
      await this.bumpDraftRevision(setId);
      return record;
   }

   async updateCase(
      id: string,
      patch: Partial<
         Pick<
            EvalCase,
            | "question"
            | "split"
            | "tags"
            | "state"
            | "source"
            | "selection"
            | "golden"
            | "artifactRefs"
         >
      >,
   ): Promise<EvalCase> {
      const current = await this.getCase(id);
      const fields = definedPatch(patch);
      const goldenChanged = fields.golden !== undefined;
      const next: EvalCase = {
         ...current,
         ...fields,
         goldenRevision: goldenChanged
            ? current.goldenRevision + 1
            : current.goldenRevision,
         updatedAt: nowIso(),
      };
      await this.db.run(
         `UPDATE eval_cases SET
            question = CAST(? AS VARCHAR),
            split = CAST(? AS VARCHAR),
            tags_json = CAST(? AS VARCHAR),
            state = CAST(? AS VARCHAR),
            source_json = CAST(? AS VARCHAR),
            selection_json = CAST(? AS VARCHAR),
            golden_json = CAST(? AS VARCHAR),
            golden_revision = CAST(? AS INTEGER),
            artifact_refs_json = CAST(? AS VARCHAR),
            updated_at = CAST(? AS TIMESTAMP)
          WHERE id = CAST(? AS VARCHAR)`,
         [
            next.question,
            next.split ?? "",
            jsonCol(next.tags),
            next.state,
            jsonCol(next.source),
            jsonCol(next.selection),
            jsonCol(next.golden),
            next.goldenRevision,
            jsonCol(next.artifactRefs),
            next.updatedAt,
            id,
         ],
      );
      await this.bumpDraftRevision(current.setId);
      return next;
   }

   async createEvidence(input: {
      setId: string;
      caseId?: string;
      kind: string;
      snippet?: string;
      localRef?: string;
      sensitivity?: string;
      redaction?: Record<string, unknown>;
   }): Promise<EvalEvidence> {
      await this.getSet(input.setId);
      const record: EvalEvidence = {
         id: crypto.randomUUID(),
         setId: input.setId,
         caseId: input.caseId,
         kind: input.kind,
         snippet: input.snippet,
         localRef: input.localRef,
         sensitivity: input.sensitivity,
         redaction: input.redaction,
         createdAt: nowIso(),
      };
      await this.db.run(
         `INSERT INTO eval_evidence (
            id, set_id, case_id, kind, snippet, local_ref, sensitivity,
            redaction_json, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
         [
            record.id,
            record.setId,
            record.caseId ?? null,
            record.kind,
            record.snippet ?? null,
            record.localRef ?? null,
            record.sensitivity ?? null,
            record.redaction ? JSON.stringify(record.redaction) : null,
            record.createdAt,
         ],
      );
      return record;
   }

   async listEvidence(setId: string): Promise<EvalEvidence[]> {
      await this.getSet(setId);
      const rows = await this.db.all<Record<string, unknown>>(
         `SELECT * FROM eval_evidence WHERE set_id = ? ORDER BY created_at`,
         [setId],
      );
      return rows.map((row) => this.toEvidence(row));
   }

   async createRun(input: {
      setId: string;
      config: Record<string, unknown>;
      status?: string;
   }): Promise<EvalRun> {
      const set = await this.getSet(input.setId);
      const now = nowIso();
      const run: EvalRun = {
         id: crypto.randomUUID(),
         setId: input.setId,
         setRevision: set.draftRevision,
         config: input.config,
         status: input.status ?? "running",
         createdAt: now,
         updatedAt: now,
      };
      await this.db.run(
         `INSERT INTO eval_runs (
            id, set_id, set_revision, config_json, status, summary_json,
            created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)`,
         [
            run.id,
            run.setId,
            run.setRevision,
            JSON.stringify(run.config),
            run.status,
            run.createdAt,
            run.updatedAt,
         ],
      );
      return run;
   }

   async getRun(id: string): Promise<EvalRun> {
      const row = await this.db.get<Record<string, unknown>>(
         `SELECT * FROM eval_runs WHERE id = ?`,
         [id],
      );
      if (!row) throw new NotFoundError(`Eval run not found: ${id}`);
      return this.toRun(row);
   }

   async listRuns(filter?: {
      setId?: string;
      status?: string;
   }): Promise<EvalRun[]> {
      const rows = filter?.setId
         ? await this.db.all<Record<string, unknown>>(
              `SELECT * FROM eval_runs WHERE set_id = ? ORDER BY created_at DESC`,
              [filter.setId],
           )
         : await this.db.all<Record<string, unknown>>(
              `SELECT * FROM eval_runs ORDER BY created_at DESC`,
           );
      const runs = rows.map((row) => this.toRun(row));
      return filter?.status
         ? runs.filter((run) => run.status === filter.status)
         : runs;
   }

   async updateRun(
      id: string,
      patch: { status?: string; summary?: Record<string, unknown> },
   ): Promise<EvalRun> {
      const current = await this.getRun(id);
      const next: EvalRun = {
         ...current,
         ...definedPatch(patch),
         updatedAt: nowIso(),
      };
      await this.db.run(
         `UPDATE eval_runs SET
            status = CAST(? AS VARCHAR),
            summary_json = CAST(? AS VARCHAR),
            updated_at = CAST(? AS TIMESTAMP)
          WHERE id = CAST(? AS VARCHAR)`,
         [
            next.status,
            jsonCol(next.summary),
            next.updatedAt,
            id,
         ],
      );
      return next;
   }

   async appendEvent(input: {
      runId: string;
      caseId?: string;
      kind: EvalEventKind;
      payload: Record<string, unknown>;
   }): Promise<EvalEvent> {
      await this.getRun(input.runId);
      const event: EvalEvent = {
         id: crypto.randomUUID(),
         runId: input.runId,
         caseId: input.caseId,
         kind: input.kind,
         payload: input.payload,
         createdAt: nowIso(),
      };
      await this.db.run(
         `INSERT INTO eval_events (id, run_id, case_id, kind, payload_json, created_at)
          VALUES (?, ?, ?, ?, ?, ?)`,
         [
            event.id,
            event.runId,
            event.caseId ?? null,
            event.kind,
            JSON.stringify(event.payload),
            event.createdAt,
         ],
      );
      return event;
   }

   async listEvents(runId: string): Promise<EvalEvent[]> {
      await this.getRun(runId);
      const rows = await this.db.all<Record<string, unknown>>(
         `SELECT * FROM eval_events WHERE run_id = ? ORDER BY created_at`,
         [runId],
      );
      return rows.map((row) => this.toEvent(row));
   }

   async exportSnapshot(setId: string): Promise<EvalSnapshot> {
      const set = await this.getSet(setId);
      const cases = await this.listCases(setId);
      const selected = cases.filter((c) => c.state !== "excluded");
      return {
         eval: {
            name: set.name,
            description: set.description,
            version: set.draftRevision,
            targetModelPath: set.targetModelPath,
            environmentName: set.environmentName,
            packageName: set.packageName,
            casesPath: "cases.jsonl",
            splits: [
               ...new Set(
                  selected.map((c) => c.split).filter((s): s is string => !!s),
               ),
            ],
         },
         cases: selected.map((c) => ({
            qid: c.qid,
            question: c.question,
            split: c.split,
            tags: c.tags,
            source: c.source,
            selection: c.selection,
            golden: c.golden
               ? { ...c.golden, revision: c.goldenRevision }
               : { status: "missing", revision: c.goldenRevision },
         })),
      };
   }

   async writeSnapshotToDirectory(
      setId: string,
      destinationRoot: string,
   ): Promise<{ directory: string; snapshot: EvalSnapshot }> {
      const snapshot = await this.exportSnapshot(setId);
      assertSafeSetName(snapshot.eval.name);
      const directory = safeJoinUnderRoot(
         destinationRoot,
         path.join("evals", snapshot.eval.name),
      );
      fs.mkdirSync(directory, { recursive: true });
      fs.writeFileSync(
         path.join(directory, "eval.json"),
         JSON.stringify(snapshot.eval, null, 2) + "\n",
      );
      fs.writeFileSync(
         path.join(directory, "cases.jsonl"),
         snapshot.cases.map((c) => JSON.stringify(c)).join("\n") +
            (snapshot.cases.length > 0 ? "\n" : ""),
      );
      await this.db.run(
         `UPDATE eval_sets SET export_revision = draft_revision, updated_at = ? WHERE id = ?`,
         [nowIso(), setId],
      );
      return { directory, snapshot };
   }

   async importSnapshot(snapshot: EvalSnapshot): Promise<EvalSet> {
      if (!snapshot?.eval?.name || !Array.isArray(snapshot.cases)) {
         throw new BadRequestError("Snapshot must include eval.name and cases");
      }
      const set = await this.createSet({
         name: snapshot.eval.name,
         description: snapshot.eval.description,
         targetModelPath: snapshot.eval.targetModelPath,
         environmentName: snapshot.eval.environmentName,
         packageName: snapshot.eval.packageName,
         metadata: { importedFromVersion: snapshot.eval.version },
      });
      for (const raw of snapshot.cases) {
         const qid = String(raw.qid ?? "");
         const question = String(raw.question ?? "");
         await this.createCase(set.id, {
            qid,
            question,
            split: typeof raw.split === "string" ? raw.split : undefined,
            tags: Array.isArray(raw.tags)
               ? raw.tags.map((t) => String(t))
               : undefined,
            state: "selected",
            source:
               raw.source && typeof raw.source === "object"
                  ? (raw.source as Record<string, unknown>)
                  : undefined,
            selection:
               raw.selection && typeof raw.selection === "object"
                  ? (raw.selection as Record<string, unknown>)
                  : undefined,
            golden:
               raw.golden && typeof raw.golden === "object"
                  ? (raw.golden as Record<string, unknown>)
                  : undefined,
         });
      }
      return this.getSet(set.id);
   }

   async importFromDirectory(directory: string): Promise<EvalSet> {
      const evalPath = path.join(directory, "eval.json");
      const casesPath = path.join(directory, "cases.jsonl");
      if (!fs.existsSync(evalPath) || !fs.existsSync(casesPath)) {
         throw new BadRequestError(
            "Import directory must contain eval.json and cases.jsonl",
         );
      }
      const evalJson = JSON.parse(fs.readFileSync(evalPath, "utf-8")) as EvalSnapshot["eval"];
      const cases = fs
         .readFileSync(casesPath, "utf-8")
         .split("\n")
         .filter((line) => line.trim().length > 0)
         .map((line) => JSON.parse(line) as Record<string, unknown>);
      return this.importSnapshot({ eval: evalJson, cases });
   }

   async listCheckpoints(filter?: {
      runId?: string;
   }): Promise<EvalCheckpoint[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         `SELECT * FROM eval_checkpoints ORDER BY created_at DESC`,
      );
      return rows
         .map((row) => this.toCheckpoint(row))
         .filter((cp) => (filter?.runId ? cp.runId === filter.runId : true))
         .map(withoutFileContent);
   }

   async getCheckpoint(id: string): Promise<EvalCheckpoint> {
      const row = await this.db.get<Record<string, unknown>>(
         `SELECT * FROM eval_checkpoints WHERE id = ?`,
         [id],
      );
      if (!row) throw new NotFoundError(`Eval checkpoint not found: ${id}`);
      return this.toCheckpoint(row);
   }

   async createCheckpoint(input: {
      label: string;
      runId?: string;
      environmentName: string;
      packageName: string;
      modelPath: string;
      servedRevision?: string;
      sourceContentSha?: string;
      issueIds?: string[];
      files: Array<{ path: string; content: string; sha256?: string }>;
   }): Promise<EvalCheckpoint> {
      const label = input.label.trim();
      if (!label) {
         throw new BadRequestError("A checkpoint needs a label");
      }
      if (input.runId) {
         await this.getRun(input.runId);
      }
      const files = normalizeCheckpointFiles(input.files);
      assertSafeRelativeModelPath(input.modelPath);
      const now = nowIso();
      const checkpoint: EvalCheckpoint = {
         id: crypto.randomUUID(),
         label,
         runId: input.runId,
         environmentName: input.environmentName,
         packageName: input.packageName,
         modelPath: input.modelPath,
         servedRevision: input.servedRevision,
         sourceContentSha: input.sourceContentSha,
         issueIds: (input.issueIds ?? []).map((id) => String(id)),
         files,
         createdAt: now,
      };
      await this.db.run(
         `INSERT INTO eval_checkpoints (
            id, label, run_id, environment_name, package_name, model_path,
            served_revision, source_content_sha, issue_ids_json, files_json,
            created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
         [
            checkpoint.id,
            checkpoint.label,
            checkpoint.runId ?? null,
            checkpoint.environmentName,
            checkpoint.packageName,
            checkpoint.modelPath,
            checkpoint.servedRevision ?? null,
            checkpoint.sourceContentSha ?? null,
            jsonCol(checkpoint.issueIds),
            jsonCol(checkpoint.files),
            checkpoint.createdAt,
         ],
      );
      return checkpoint;
   }

   async reset(): Promise<void> {
      await this.db.run(`DELETE FROM eval_events`);
      await this.db.run(`DELETE FROM eval_runs`);
      await this.db.run(`DELETE FROM eval_evidence`);
      await this.db.run(`DELETE FROM eval_cases`);
      await this.db.run(`DELETE FROM eval_sets`);
      await this.db.run(`DELETE FROM eval_checkpoints`);
   }

   private async assertNoActiveName(
      name: string,
      exceptId?: string,
   ): Promise<void> {
      const clash = (await this.listSets({ status: "active", name })).find(
         (set) => set.id !== exceptId,
      );
      if (clash) {
         throw new ConflictError(
            `An active eval set named '${name}' already exists (${clash.id}). Archive it or PATCH goldens on that row; do not import a second copy.`,
         );
      }
   }

   private async bumpDraftRevision(setId: string): Promise<void> {
      await this.db.run(
         `UPDATE eval_sets SET draft_revision = draft_revision + 1, updated_at = ? WHERE id = ?`,
         [nowIso(), setId],
      );
   }

   private toSet(row: Record<string, unknown>): EvalSet {
      return {
         id: String(row.id),
         name: String(row.name),
         description: (row.description as string | null) ?? undefined,
         targetModelPath: (row.target_model_path as string | null) ?? undefined,
         environmentName: (row.environment_name as string | null) ?? undefined,
         packageName: (row.package_name as string | null) ?? undefined,
         draftRevision: Number(row.draft_revision),
         exportRevision:
            row.export_revision === null || row.export_revision === undefined
               ? undefined
               : Number(row.export_revision),
         metadata: parseJson(row.metadata_json as string | null, undefined),
         status: parseSetStatus(row.status),
         createdAt: String(row.created_at),
         updatedAt: String(row.updated_at),
      };
   }

   private toCase(row: Record<string, unknown>): EvalCase {
      return {
         id: String(row.id),
         setId: String(row.set_id),
         qid: String(row.qid),
         question: String(row.question),
         split: (row.split as string | null) ?? undefined,
         tags: parseJson(row.tags_json as string | null, undefined),
         state: String(row.state) as EvalCaseState,
         source: parseJson(row.source_json as string | null, undefined),
         selection: parseJson(row.selection_json as string | null, undefined),
         golden: parseJson(row.golden_json as string | null, undefined),
         goldenRevision: Number(row.golden_revision ?? 0),
         artifactRefs: parseJson(
            row.artifact_refs_json as string | null,
            undefined,
         ),
         createdAt: String(row.created_at),
         updatedAt: String(row.updated_at),
      };
   }

   private toEvidence(row: Record<string, unknown>): EvalEvidence {
      return {
         id: String(row.id),
         setId: String(row.set_id),
         caseId: (row.case_id as string | null) ?? undefined,
         kind: String(row.kind),
         snippet: (row.snippet as string | null) ?? undefined,
         localRef: (row.local_ref as string | null) ?? undefined,
         sensitivity: (row.sensitivity as string | null) ?? undefined,
         redaction: parseJson(row.redaction_json as string | null, undefined),
         createdAt: String(row.created_at),
      };
   }

   private toRun(row: Record<string, unknown>): EvalRun {
      return {
         id: String(row.id),
         setId: String(row.set_id),
         setRevision: Number(row.set_revision),
         config: parseJson(row.config_json as string | null, {}),
         status: String(row.status),
         summary: parseJson(row.summary_json as string | null, undefined),
         createdAt: String(row.created_at),
         updatedAt: String(row.updated_at),
      };
   }

   private toEvent(row: Record<string, unknown>): EvalEvent {
      return {
         id: String(row.id),
         runId: String(row.run_id),
         caseId: (row.case_id as string | null) ?? undefined,
         kind: String(row.kind) as EvalEventKind,
         payload: parseJson(row.payload_json as string | null, {}),
         createdAt: String(row.created_at),
      };
   }

   private toCheckpoint(row: Record<string, unknown>): EvalCheckpoint {
      return {
         id: String(row.id),
         label: String(row.label),
         runId: (row.run_id as string | null) ?? undefined,
         environmentName: String(row.environment_name),
         packageName: String(row.package_name),
         modelPath: String(row.model_path),
         servedRevision: (row.served_revision as string | null) ?? undefined,
         sourceContentSha:
            (row.source_content_sha as string | null) ?? undefined,
         issueIds: parseJson(row.issue_ids_json as string | null, []),
         files: parseJson(row.files_json as string | null, []),
         createdAt: String(row.created_at),
      };
   }
}

export function assertSafeEvalExportRoot(root: string): void {
   if (!path.isAbsolute(root)) {
      throw new BadRequestError("Export destination must be an absolute path");
   }
}
