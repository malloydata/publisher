import fs from "fs";
import path from "path";
import { getEvalStoreEnabled } from "../config";
import { PUBLISHER_DATA_DIR } from "../constants";
import { BadRequestError, NotFoundError } from "../errors";
import {
   assertSafePackageName,
   assertSafeRelativeModelPath,
   safeJoinUnderRoot,
} from "../path_safety";
import {
   EvalStore,
   type EvalCheckpointFile,
   type EvalEventKind,
   type EvalSnapshot,
} from "../service/eval_store";
import type { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";

function requireEnabled(): void {
   if (!getEvalStoreEnabled()) {
      throw new NotFoundError(
         "Eval store is disabled. Set PUBLISHER_EVAL_STORE=on.",
      );
   }
}

function asObject(body: unknown): Record<string, unknown> {
   if (!body || typeof body !== "object" || Array.isArray(body)) {
      throw new BadRequestError("Expected a JSON object body");
   }
   return body as Record<string, unknown>;
}

export class EvalController {
   constructor(
      private getDb: () => DuckDBConnection,
      private options?: {
         serverRoot?: string;
         reloadPackage?: (
            environmentName: string,
            packageName: string,
         ) => Promise<{
            metadata: { servedRevision?: string; sourceContentSha?: string };
            mode: string;
         }>;
      },
   ) {}

   private store(): EvalStore {
      requireEnabled();
      return new EvalStore(this.getDb());
   }

   listSets = async (status?: string, name?: string) => {
      const parsed =
         status === "all" || status === "archived" || status === "active"
            ? status
            : undefined;
      return this.store().listSets({
         status: parsed,
         name: name && name.length > 0 ? name : undefined,
      });
   };
   getSet = async (id: string) => this.store().getSet(id);
   createSet = async (body: unknown) => {
      const input = asObject(body);
      return this.store().createSet({
         name: String(input.name ?? ""),
         description:
            typeof input.description === "string"
               ? input.description
               : undefined,
         targetModelPath:
            typeof input.targetModelPath === "string"
               ? input.targetModelPath
               : undefined,
         environmentName:
            typeof input.environmentName === "string"
               ? input.environmentName
               : undefined,
         packageName:
            typeof input.packageName === "string" ? input.packageName : undefined,
         metadata:
            input.metadata && typeof input.metadata === "object"
               ? (input.metadata as Record<string, unknown>)
               : undefined,
      });
   };
   updateSet = async (id: string, body: unknown) => {
      const input = asObject(body);
      return this.store().updateSet(id, {
         name: typeof input.name === "string" ? input.name : undefined,
         description:
            typeof input.description === "string"
               ? input.description
               : undefined,
         targetModelPath:
            typeof input.targetModelPath === "string"
               ? input.targetModelPath
               : undefined,
         environmentName:
            typeof input.environmentName === "string"
               ? input.environmentName
               : undefined,
         packageName:
            typeof input.packageName === "string" ? input.packageName : undefined,
         metadata:
            input.metadata && typeof input.metadata === "object"
               ? (input.metadata as Record<string, unknown>)
               : undefined,
         status:
            input.status === "active" || input.status === "archived"
               ? input.status
               : undefined,
      });
   };

   listCases = async (setId: string) => this.store().listCases(setId);
   getCase = async (id: string) => this.store().getCase(id);
   createCase = async (setId: string, body: unknown) => {
      const input = asObject(body);
      return this.store().createCase(setId, {
         qid: String(input.qid ?? ""),
         question: String(input.question ?? ""),
         split: typeof input.split === "string" ? input.split : undefined,
         tags: Array.isArray(input.tags)
            ? input.tags.map((t) => String(t))
            : undefined,
         state:
            input.state === "candidate" ||
            input.state === "selected" ||
            input.state === "excluded"
               ? input.state
               : undefined,
         source:
            input.source && typeof input.source === "object"
               ? (input.source as Record<string, unknown>)
               : undefined,
         selection:
            input.selection && typeof input.selection === "object"
               ? (input.selection as Record<string, unknown>)
               : undefined,
         golden:
            input.golden && typeof input.golden === "object"
               ? (input.golden as Record<string, unknown>)
               : undefined,
      });
   };
   updateCase = async (id: string, body: unknown) => {
      const input = asObject(body);
      return this.store().updateCase(id, {
         question:
            typeof input.question === "string" ? input.question : undefined,
         split: typeof input.split === "string" ? input.split : undefined,
         tags: Array.isArray(input.tags)
            ? input.tags.map((t) => String(t))
            : undefined,
         state:
            input.state === "candidate" ||
            input.state === "selected" ||
            input.state === "excluded"
               ? input.state
               : undefined,
         source:
            input.source && typeof input.source === "object"
               ? (input.source as Record<string, unknown>)
               : undefined,
         selection:
            input.selection && typeof input.selection === "object"
               ? (input.selection as Record<string, unknown>)
               : undefined,
         golden:
            input.golden && typeof input.golden === "object"
               ? (input.golden as Record<string, unknown>)
               : undefined,
      });
   };

   listEvidence = async (setId: string) => this.store().listEvidence(setId);
   createEvidence = async (setId: string, body: unknown) => {
      const input = asObject(body);
      return this.store().createEvidence({
         setId,
         caseId: typeof input.caseId === "string" ? input.caseId : undefined,
         kind: String(input.kind ?? "note"),
         snippet: typeof input.snippet === "string" ? input.snippet : undefined,
         localRef:
            typeof input.localRef === "string" ? input.localRef : undefined,
         sensitivity:
            typeof input.sensitivity === "string"
               ? input.sensitivity
               : undefined,
      });
   };

   listRuns = async (setId?: string, status?: string) =>
      this.store().listRuns({ setId, status });
   getRun = async (id: string) => this.store().getRun(id);
   createRun = async (body: unknown) => {
      const input = asObject(body);
      return this.store().createRun({
         setId: String(input.setId ?? ""),
         config:
            input.config && typeof input.config === "object"
               ? (input.config as Record<string, unknown>)
               : {},
         status: typeof input.status === "string" ? input.status : undefined,
      });
   };
   updateRun = async (id: string, body: unknown) => {
      const input = asObject(body);
      return this.store().updateRun(id, {
         status: typeof input.status === "string" ? input.status : undefined,
         summary:
            input.summary && typeof input.summary === "object"
               ? (input.summary as Record<string, unknown>)
               : undefined,
      });
   };

   listEvents = async (runId: string) => this.store().listEvents(runId);
   appendEvent = async (runId: string, body: unknown) => {
      const input = asObject(body);
      const kind = String(input.kind ?? "") as EvalEventKind;
      const allowed: EvalEventKind[] = [
         "attempt",
         "tool_call",
         "score",
         "issue",
         "issue_status",
         "candidate",
         "gate",
         "checkpoint",
      ];
      if (!allowed.includes(kind)) {
         throw new BadRequestError(`Invalid event kind: ${kind}`);
      }
      return this.store().appendEvent({
         runId,
         caseId: typeof input.caseId === "string" ? input.caseId : undefined,
         kind,
         payload:
            input.payload && typeof input.payload === "object"
               ? (input.payload as Record<string, unknown>)
               : {},
      });
   };

   exportSet = async (setId: string, body: unknown) => {
      const input = asObject(body);
      if (typeof input.destinationPath === "string") {
         return this.store().writeSnapshotToDirectory(
            setId,
            input.destinationPath,
         );
      }
      return { snapshot: await this.store().exportSnapshot(setId) };
   };

   importSet = async (body: unknown) => {
      const input = asObject(body);
      if (typeof input.directory === "string") {
         return this.store().importFromDirectory(input.directory);
      }
      if (input.snapshot && typeof input.snapshot === "object") {
         return this.store().importSnapshot(input.snapshot as EvalSnapshot);
      }
      throw new BadRequestError("Provide snapshot or directory");
   };

   listCheckpoints = async (runId?: string) =>
      this.store().listCheckpoints({
         runId: runId && runId.length > 0 ? runId : undefined,
      });

   getCheckpoint = async (id: string) => this.store().getCheckpoint(id);

   createCheckpoint = async (body: unknown) => {
      const input = asObject(body);
      const environmentName = String(input.environmentName ?? "");
      const packageName = String(input.packageName ?? "");
      const modelPath = String(input.modelPath ?? "");
      assertSafePackageName(environmentName);
      assertSafePackageName(packageName);
      assertSafeRelativeModelPath(modelPath);
      const files = this.resolveCheckpointFiles(input, {
         environmentName,
         packageName,
         modelPath,
      });
      const store = this.store();
      const checkpoint = await store.createCheckpoint({
         label: String(input.label ?? ""),
         runId: typeof input.runId === "string" ? input.runId : undefined,
         environmentName,
         packageName,
         modelPath,
         servedRevision:
            typeof input.servedRevision === "string"
               ? input.servedRevision
               : undefined,
         sourceContentSha:
            typeof input.sourceContentSha === "string"
               ? input.sourceContentSha
               : undefined,
         issueIds: Array.isArray(input.issueIds)
            ? input.issueIds.map((id) => String(id))
            : undefined,
         files,
      });
      this.writeCheckpointMirror(checkpoint.id, checkpoint.files);
      if (checkpoint.runId) {
         await store.appendEvent({
            runId: checkpoint.runId,
            kind: "checkpoint",
            payload: {
               action: "created",
               checkpointId: checkpoint.id,
               label: checkpoint.label,
               servedRevision: checkpoint.servedRevision,
               issueIds: checkpoint.issueIds,
            },
         });
      }
      return checkpoint;
   };

   restoreCheckpoint = async (id: string) => {
      const store = this.store();
      const checkpoint = await store.getCheckpoint(id);
      const pkgDir = this.packageDir(
         checkpoint.environmentName,
         checkpoint.packageName,
      );
      for (const file of checkpoint.files) {
         const dest = safeJoinUnderRoot(pkgDir, file.path);
         fs.mkdirSync(path.dirname(dest), { recursive: true });
         fs.writeFileSync(dest, file.content);
      }
      let reload:
         | {
              mode: string;
              servedRevision?: string;
              sourceContentSha?: string;
           }
         | undefined;
      if (this.options?.reloadPackage) {
         const result = await this.options.reloadPackage(
            checkpoint.environmentName,
            checkpoint.packageName,
         );
         reload = {
            mode: result.mode,
            servedRevision: result.metadata.servedRevision,
            sourceContentSha: result.metadata.sourceContentSha,
         };
      }
      if (checkpoint.runId) {
         await store.appendEvent({
            runId: checkpoint.runId,
            kind: "checkpoint",
            payload: {
               action: "restored",
               checkpointId: checkpoint.id,
               label: checkpoint.label,
               servedRevision: reload?.servedRevision ?? checkpoint.servedRevision,
            },
         });
      }
      return { checkpoint: withoutContent(checkpoint), reload };
   };

   reset = async () => {
      await this.store().reset();
      return { reset: true };
   };

   private packageDir(environmentName: string, packageName: string): string {
      const serverRoot = this.options?.serverRoot;
      if (!serverRoot) {
         throw new BadRequestError(
            "Checkpoint restore needs a server root to write package files",
         );
      }
      assertSafePackageName(environmentName);
      assertSafePackageName(packageName);
      return safeJoinUnderRoot(
         serverRoot,
         PUBLISHER_DATA_DIR,
         environmentName,
         packageName,
      );
   }

   private resolveCheckpointFiles(
      input: Record<string, unknown>,
      names: {
         environmentName: string;
         packageName: string;
         modelPath: string;
      },
   ): Array<{ path: string; content: string }> {
      if (Array.isArray(input.files) && input.files.length > 0) {
         return input.files.map((raw) => {
            const file = asObject(raw);
            return {
               path: String(file.path ?? ""),
               content: String(file.content ?? ""),
            };
         });
      }
      const paths = new Set<string>([names.modelPath]);
      if (Array.isArray(input.paths)) {
         for (const extra of input.paths) {
            paths.add(String(extra));
         }
      }
      const pkgDir = this.packageDir(names.environmentName, names.packageName);
      return [...paths].map((relative) => {
         assertSafeRelativeModelPath(relative);
         const abs = safeJoinUnderRoot(pkgDir, relative);
         if (!fs.existsSync(abs)) {
            throw new BadRequestError(
               `Checkpoint file not found on disk: ${relative}`,
            );
         }
         return { path: relative, content: fs.readFileSync(abs, "utf-8") };
      });
   }

   private writeCheckpointMirror(
      checkpointId: string,
      files: EvalCheckpointFile[],
   ): void {
      const serverRoot = this.options?.serverRoot;
      if (!serverRoot) return;
      const destRoot = safeJoinUnderRoot(
         serverRoot,
         ".eval_checkpoints",
         checkpointId,
      );
      for (const file of files) {
         const dest = safeJoinUnderRoot(destRoot, file.path);
         fs.mkdirSync(path.dirname(dest), { recursive: true });
         fs.writeFileSync(dest, file.content);
      }
   }
}

function withoutContent(checkpoint: {
   files: EvalCheckpointFile[];
   [key: string]: unknown;
}) {
   return {
      ...checkpoint,
      files: checkpoint.files.map((file) => ({
         path: file.path,
         sha256: file.sha256,
      })),
   };
}
