// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryClient, QueryKey } from "@tanstack/react-query";
import { parseResourceUri } from "../../utils/formatting";
import { sha256Hex } from "../../utils/sha256";
import type { Workspace } from "../DocumentStorage";
import type { ApiClients } from "../ServerProvider";

// The pure half of an editor host's session with its document: which
// workspace it opens against, where Save goes, and the hash a package write
// carries. The dashboard and notebook hosts both decide these the same way.

/** Where a host's Save lands. */
export type SavesTo = "package" | "browser" | "host";

/**
 * The record when one declares itself, and otherwise the first writeable
 * workspace. Pass every workspace, not only the writeable ones: a reader who
 * cannot write to the record still has to be shown the record.
 */
export function chooseWorkspace(
   all: readonly Workspace[],
): Workspace | undefined {
   return (
      all.find((candidate) => candidate.authoritative) ??
      all.find((candidate) => candidate.writeable)
   );
}

/**
 * Where Save goes, and which writer takes it, or `undefined` for no Save.
 *
 * `authoritative` wins outright rather than breaking a tie: a host whose store
 * is the record may well sit on a server that reports itself writable, and
 * writing the package there would edit a deploy of the record instead of the
 * record.
 */
export function saveTarget({
   authoritative,
   mutable,
   versionId,
   canStore,
   readFailed,
}: {
   authoritative: boolean;
   mutable: boolean;
   versionId?: string;
   /** The host keeps documents and the chosen workspace takes writes. */
   canStore: boolean;
   /** The host's copy could not be read. */
   readFailed: boolean;
}): {
   savesTo: SavesTo;
   pinnedPackageSave: boolean;
   writer: "storage" | "package" | undefined;
} {
   const savesTo = authoritative ? "host" : mutable ? "package" : "browser";
   // A version is an immutable checkpoint, and `updateModelSource` cannot be
   // told to write against one (the server answers 501). Without this guard,
   // `expectedHash` would be the hash of the pinned text, the server would
   // refuse every save against the current file, and the catch would refetch
   // the same pinned text: a dead end with no way out. Storage-backed saves are
   // unaffected, since they never touch the package's compare-and-swap.
   const pinnedPackageSave = versionId !== undefined && savesTo === "package";
   const writer = authoritative
      ? canStore
         ? "storage"
         : undefined
      : pinnedPackageSave
        ? undefined
        : mutable
          ? "package"
          : canStore
            ? "storage"
            : undefined;
   // A copy that could not be read is not a copy that is not there. Saving on
   // that belief is what rewinds the record, so Save is off until a reader can
   // be told what actually happened.
   return {
      savesTo,
      pinnedPackageSave,
      writer: readFailed ? undefined : writer,
   };
}

/**
 * The `expectedHash` a package write carries: the server's own hash after this
 * editor's last write, else the hash of the package file the editor opened on.
 * Never the latest fetch's, which may already hold someone else's change.
 */
export async function expectedHashFor(
   savedHash: string | undefined,
   base: string | undefined,
): Promise<string | undefined> {
   return savedHash ?? (base === undefined ? undefined : await sha256Hex(base));
}

/**
 * What the toolbar says about where Save goes, in the backend's own words
 * wherever it has any: a workspace carries a `description` precisely so the
 * editor does not have to guess, and "this browser" is one host's answer
 * rather than the interface's.
 */
export function saveCaption({
   authoritative,
   mutable,
   pinnedPackageSave,
   workspace,
   readFailure,
   versionId,
}: {
   authoritative: boolean;
   mutable: boolean;
   pinnedPackageSave: boolean;
   workspace?: Workspace;
   readFailure?: string;
   versionId?: string;
}): string {
   if (readFailure !== undefined)
      return `The saved copy could not be read, so Save is off: ${readFailure}`;
   if (authoritative && workspace)
      return workspace.writeable
         ? workspace.description
         : `${workspace.description}: you cannot save into it.`;
   if (pinnedPackageSave)
      return `Reading version ${versionId}: a version is a fixed point in history, so Save is off.`;
   if (mutable) return "Save writes the file into the package.";
   if (workspace)
      return `${workspace.description}: this server does not take writes.`;
   return "This server does not take writes.";
}

/** What a storage backend said went wrong, for a reader who has to act on it. */
export function storageErrorMessage(error: unknown): string {
   return error instanceof Error ? error.message : String(error);
}

/** The server's own reason for a refused write, when it gave one. */
export function apiErrorMessage(error: unknown): string {
   const data = (error as { response?: { data?: { message?: string } } })
      .response?.data;
   if (data?.message) return data.message;
   return error instanceof Error ? error.message : String(error);
}

/** A query family a package write marks stale. */
export interface Invalidation {
   queryKey: QueryKey;
   /** `"none"` marks the family stale without refetching it. */
   refetchType?: "none";
   /** Hold the write's result until the refetch lands. */
   wait?: boolean;
}

/**
 * Writes a package file against `expectedHash` and returns the server's hash
 * of what it wrote. A refused write invalidates `invalidateOnError`, since it
 * usually means the file moved, and throws the server's own reason.
 * `afterWrite` runs once the write has landed and before the invalidations, so
 * the caller's bookkeeping is in place when a refetch arrives.
 */
export async function writePackageFile({
   apiClients,
   queryClient,
   environmentName,
   packageName,
   modelPath,
   source,
   expectedHash,
   invalidateOnError,
   invalidate,
   afterWrite,
}: {
   apiClients: ApiClients;
   queryClient: QueryClient;
   environmentName: string;
   packageName: string;
   modelPath: string;
   source: string;
   expectedHash: string;
   invalidateOnError: readonly QueryKey[];
   invalidate: readonly Invalidation[];
   afterWrite?: (contentHash: string) => Promise<void> | void;
}): Promise<string> {
   let result;
   try {
      result = await apiClients.models.updateModelSource(
         environmentName,
         packageName,
         modelPath,
         { source, expectedHash },
      );
   } catch (error) {
      for (const queryKey of invalidateOnError)
         void queryClient.invalidateQueries({ queryKey });
      throw new Error(apiErrorMessage(error));
   }
   const contentHash = result.data.contentHash;
   await afterWrite?.(contentHash);
   for (const { queryKey, refetchType, wait } of invalidate) {
      const pending = queryClient.invalidateQueries({
         queryKey,
         ...(refetchType ? { refetchType } : {}),
      });
      if (wait) await pending;
      else void pending;
   }
   return contentHash;
}

/** The host workspace that took a save or its undo, named on the event; a package save was taken by none. */
export function withWorkspace<E extends { type: string }>(
   event: E,
   workspace: string | undefined,
): E {
   if (
      workspace === undefined ||
      !/\.(saved|save_undone)$/.test(event.type) ||
      (event as { where?: SavesTo }).where === "package"
   )
      return event;
   return { ...event, workspace };
}

/** The environment, package and version an editor was asked for, from either prop form; a render body must not throw, so a URI `parseResourceUri` rejects reads as naming neither. */
export function resolveEditorTarget(
   props:
      | { resourceUri: string }
      | { environmentName: string; packageName: string },
): {
   environmentName: string;
   packageName: string;
   versionId: string | undefined;
   /** False when a resource URI did not name both an environment and a package. */
   namesBoth: boolean;
} {
   if (!("resourceUri" in props))
      return {
         environmentName: props.environmentName,
         packageName: props.packageName,
         versionId: undefined,
         namesBoth: true,
      };
   let parsed: ReturnType<typeof parseResourceUri> | undefined;
   try {
      parsed = parseResourceUri(props.resourceUri);
   } catch {
      parsed = undefined;
   }
   return {
      environmentName: parsed?.environmentName ?? "",
      packageName: parsed?.packageName ?? "",
      versionId: parsed?.versionId,
      namesBoth: !!parsed?.environmentName && !!parsed?.packageName,
   };
}
