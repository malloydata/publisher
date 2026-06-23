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

/**
 * Human-friendly status label. The publisher drives every phase automatically,
 * so the intermediate protocol states (PENDING / BUILD_PLAN_READY /
 * MANIFEST_ROWS_READY) all read as "Pending" and the terminal success state
 * reads as "Done". Failures and cancellations keep their own labels.
 */
export function statusLabel(status?: MaterializationStatus): string {
   switch (status) {
      case MaterializationStatus.ManifestFileReady:
         return "Done";
      case MaterializationStatus.Failed:
         return "Failed";
      case MaterializationStatus.Cancelled:
         return "Cancelled";
      case MaterializationStatus.Pending:
      case MaterializationStatus.BuildPlanReady:
      case MaterializationStatus.ManifestRowsReady:
         return "Pending";
      default:
         return "Unknown";
   }
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

/** Absolute, locale-formatted timestamp (e.g. "Jun 19, 2026, 3:33 PM"). */
export function formatTimestamp(iso?: string | null): string {
   if (!iso) return "—";
   const date = new Date(iso);
   if (Number.isNaN(date.getTime())) return "—";
   return date.toLocaleString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
   });
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
