import { Materialization, MaterializationStatus } from "../../client";

/**
 * Non-terminal phases of the two-round protocol. The publisher kicks off Round 1
 * (PENDING -> BUILD_PLAN_READY); the control plane drives Round 2
 * (-> MANIFEST_ROWS_READY -> MANIFEST_FILE_READY). Any of these may be polled and
 * stopped.
 */
export function isActiveStatus(status?: MaterializationStatus): boolean {
   return (
      status === MaterializationStatus.Pending ||
      status === MaterializationStatus.BuildPlanReady ||
      status === MaterializationStatus.ManifestRowsReady
   );
}

export function isTerminalStatus(status?: MaterializationStatus): boolean {
   return (
      status === MaterializationStatus.ManifestFileReady ||
      status === MaterializationStatus.Failed ||
      status === MaterializationStatus.Cancelled
   );
}

type ChipColor = "default" | "info" | "success" | "error" | "warning";

export function statusColor(status?: MaterializationStatus): ChipColor {
   switch (status) {
      case MaterializationStatus.BuildPlanReady:
      case MaterializationStatus.ManifestRowsReady:
         return "info";
      case MaterializationStatus.ManifestFileReady:
         return "success";
      case MaterializationStatus.Failed:
         return "error";
      case MaterializationStatus.Cancelled:
         return "warning";
      case MaterializationStatus.Pending:
      default:
         return "default";
   }
}

/**
 * Shape of Materialization.metadata. The generated client types it as
 * `object | null`, so callers must read it through parseMetadata(), which
 * narrows defensively and tolerates a missing or differently-shaped object.
 */
export interface MaterializationMetadata {
   forceRefresh?: boolean;
   sourceNames?: string[];
   sourcesBuilt?: number;
   sourcesSkipped?: number;
}

export function parseMetadata(
   materialization: Materialization,
): MaterializationMetadata {
   return (materialization.metadata ?? {}) as MaterializationMetadata;
}

/** Human-readable elapsed time between two ISO timestamps (to now if open). */
export function formatDuration(
   startedAt?: string | null,
   completedAt?: string | null,
): string {
   if (!startedAt) return "-";
   const start = new Date(startedAt).getTime();
   const end = completedAt ? new Date(completedAt).getTime() : Date.now();
   const totalMs = Math.max(0, end - start);
   if (totalMs < 1000) return `${totalMs}ms`;
   const seconds = Math.round(totalMs / 1000);
   if (seconds < 60) return `${seconds}s`;
   const minutes = Math.floor(seconds / 60);
   const remSeconds = seconds % 60;
   if (minutes < 60) return `${minutes}m ${remSeconds}s`;
   const hours = Math.floor(minutes / 60);
   const remMinutes = minutes % 60;
   return `${hours}h ${remMinutes}m`;
}

/** Compact relative timestamp, e.g. "just now", "5m ago", "2h ago", "3d ago". */
export function formatRelativeTime(iso?: string | null): string {
   if (!iso) return "-";
   const then = new Date(iso).getTime();
   const seconds = Math.round((Date.now() - then) / 1000);
   if (seconds < 60) return "just now";
   const minutes = Math.floor(seconds / 60);
   if (minutes < 60) return `${minutes}m ago`;
   const hours = Math.floor(minutes / 60);
   if (hours < 24) return `${hours}h ago`;
   const days = Math.floor(hours / 24);
   return `${days}d ago`;
}
