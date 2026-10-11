// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { saveTarget } from "../DashboardBuilder/documentSession";
import {
   isDocumentNotFound,
   type DocumentLocator,
   type DocumentStorage,
   type Workspace,
} from "../DocumentStorage/DocumentStorage";
import type { ReportLayout } from "../Package/documentLocation";
import {
   locatorFor,
   normalizeDirectory,
   reportPathForTitle,
} from "./documentPath";
import { reportEmitter, type ReportEventHandler } from "../Report/telemetry";
import { now } from "../../utils/clock";
import { newDocumentProblem, type NewDocument } from "./guards";
import { newReportSource } from "./newNotebook";

/** How many names are tried before a create gives up: `title`, `title-2`, ... */
export const MAX_SLUG_ATTEMPTS = 20;

/**
 * Where a create may land, or `undefined` for nowhere. A non-authoritative
 * browser workspace is never a target (`savesTo === "browser"`): a document
 * created there would exist for one reader on one machine and look saved.
 */
export function createRoute(host: {
   authoritative: boolean;
   mutable: boolean;
   /** The host keeps documents and the chosen workspace takes writes. */
   canStore: boolean;
   /** A pinned version takes no package writes. */
   versionId?: string;
}): "package" | "storage" | undefined {
   const { savesTo, writer } = saveTarget({ ...host, readFailed: false });
   return savesTo === "browser" ? undefined : writer;
}

export type CreateTarget =
   | {
        route: "package";
        /** Package-relative paths already in the package, as its listing gives them. */
        existing: readonly string[];
        /** A create-only write: no hash, so the server refuses a file that is there. */
        write: (path: string, source: string) => Promise<void>;
     }
   | {
        route: "storage";
        storage: DocumentStorage;
        workspace: Workspace;
        environmentName: string;
        packageName: string;
        /** Package-relative paths the package already lists; the store may not know them yet. */
        existing?: readonly string[];
     };

export interface CreatedDocument {
   layout: ReportLayout;
   /**
    * Package-relative, e.g. `sales.malloy`: the report's identity. At the
    * package root unless a `directory` or `path` was given. On the storage
    * route this is where the store says the file lands
    * ({@link DocumentStorage.resolveDocumentPath}), which may differ from the
    * path asked for.
    */
   path: string;
   /** `sales` for `sales.malloy` or `team/sales.malloy`: a display label. */
   slug: string;
   /** Set on the storage route: the new document's address there. */
   locator?: DocumentLocator;
}

export interface CreateDocumentOptions {
   /** The report's layout. Default linear. */
   layout?: ReportLayout;
   /**
    * Exactly where to write it, package-relative with `.malloy`. A path that is
    * taken is refused rather than renamed. Absent, the title's slug is used at
    * the package root (or in `directory`), with `-2`, `-3`… when taken.
    */
   path?: string;
   /** The directory a title-named file goes in; default the package root. */
   directory?: string;
   document: NewDocument;
   target: CreateTarget;
   onCreated?: (created: CreatedDocument) => void;
   /** `report.created` with where it landed, or `report.create_refused` with why not. */
   onEvent?: ReportEventHandler;
}

const slugOf = (path: string) =>
   path.slice(path.lastIndexOf("/") + 1).replace(/\.malloy$/, "");

/** Why `path` cannot be where a new report is written, or undefined when it can. */
export function reportPathProblem(path: string): string | undefined {
   if (!path.endsWith(".malloy"))
      return "A report's file name ends in .malloy.";
   const segments = path.split("/");
   if (segments.some((s) => s === "" || s === "." || s === ".."))
      return "A location is a path inside the package, such as sales/q3.malloy.";
   if (segments.at(-1) === ".malloy") return "A report's file needs a name.";
   // Without case: on a case-insensitive file system `Public/` is `public/`.
   if (segments[0].toLowerCase() === "public")
      return "A report cannot live under public/, which is served as plain files.";
   if (path === "index.malloy")
      return "The package's index.malloy is its surface, never a report.";
   return undefined;
}

/**
 * Write a new report, never over an existing file, and say where it went: at
 * the package root unless the caller names a directory or a path. Reads
 * nothing back: the host navigates from what this returns.
 *
 * On the storage route the listing alone cannot be trusted to be current, and
 * `saveDocument` replaces what is there, so each candidate name is read first
 * and used only when the read says the document is absent.
 */
export async function createDocument(
   options: CreateDocumentOptions,
): Promise<CreatedDocument> {
   const layout: ReportLayout = options.layout ?? "linear";
   const startedAt = now();
   let created: CreatedDocument;
   try {
      created = await writeNewReport(options, layout);
   } catch (error) {
      reportEmitter({ path: options.path ?? "", layout }, options.onEvent).emit(
         {
            type: "report.create_refused",
            reason: error instanceof Error ? error.message : String(error),
         },
      );
      throw error;
   }
   reportEmitter({ path: created.path, layout }, options.onEvent).emit({
      type: "report.created",
      where: options.target.route === "package" ? "package" : "host",
      durationMs: now() - startedAt,
   });
   options.onCreated?.(created);
   return created;
}

async function writeNewReport(
   options: CreateDocumentOptions,
   layout: ReportLayout,
): Promise<CreatedDocument> {
   const { document, target } = options;
   const problem = newDocumentProblem(document);
   if (problem) throw new Error(problem);
   const directory = normalizeDirectory(options.directory);
   // The one path asked for, or the title's slug and its numbered variants. Taken is by path, and only by path.
   const candidates =
      options.path !== undefined
         ? [options.path]
         : Array.from({ length: MAX_SLUG_ATTEMPTS }, (_, i) =>
              reportPathForTitle(document.title, i + 1, directory),
           );
   // Every computed path is checked, not only a `path` given outright: a
   // `directory` of `../x` or `public` is refused here, before the package
   // or the store is asked anything.
   for (const candidate of candidates) {
      const pathProblem = reportPathProblem(candidate);
      if (pathProblem) throw new Error(pathProblem);
   }
   // The import is relative to where the report lands, so each candidate gets its own text.
   const sourceAt = (path: string) => newReportSource(document, layout, path);

   if (target.route === "package") {
      const taken = new Set(target.existing);
      let path: string | undefined;
      for (const candidate of candidates) {
         if (taken.has(candidate)) continue;
         try {
            await target.write(candidate, sourceAt(candidate));
            path = candidate;
            break;
         } catch (error) {
            // A create-only write refused as a conflict means the listing was stale; the next name is free to try.
            if (!isConflict(error)) throw error;
         }
      }
      if (path === undefined) throw noFreeName(document.title, options.path);
      return { layout, path, slug: slugOf(path) };
   }
   const { storage, workspace, environmentName, packageName } = target;
   const inPackage = new Set(target.existing);
   const listed = new Set(
      (await storage.listDocuments(workspace)).map((l) => l.path),
   );
   let found: CreatedDocument | undefined;
   for (const requested of candidates) {
      const locator = locatorFor(
         layout,
         workspace.name,
         environmentName,
         packageName,
         requested,
      );
      // Where the file really lands: the text is rendered for that path, so its imports resolve there.
      const path = (await storage.resolveDocumentPath?.(locator)) ?? requested;
      if (
         listed.has(locator.path) ||
         inPackage.has(requested) ||
         inPackage.has(path)
      )
         continue;
      let present = true;
      try {
         await storage.getDocument(locator);
      } catch (error) {
         // Only absence frees the name; a backend that could not answer says nothing about it.
         if (!isDocumentNotFound(error)) throw error;
         present = false;
      }
      if (!present) {
         found = { layout, path, slug: slugOf(path), locator };
         break;
      }
   }
   if (found === undefined) throw noFreeName(document.title, options.path);
   await storage.saveDocument(found.locator!, sourceAt(found.path));
   return found;
}

const isConflict = (error: unknown) =>
   (error as { response?: { status?: number } })?.response?.status === 409;

const noFreeName = (title: string, path?: string) =>
   new Error(
      path !== undefined
         ? `${path} already exists. Fix: choose a different location.`
         : `No free file name for "${title}" after ${MAX_SLUG_ATTEMPTS} tries. Fix: choose a different title.`,
   );
