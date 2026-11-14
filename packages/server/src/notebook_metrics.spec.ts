// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";

import {
   recordNotebookCellExecution,
   resetNotebookMetricsForTest,
} from "./notebook_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("notebook_metrics", () => {
   let harness: MetricsHarness;

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetNotebookMetricsForTest();
   });

   afterEach(async () => {
      resetNotebookMetricsForTest();
      await harness.shutdown();
   });

   it("counts a cell run by format, kind and outcome, and times it by format and outcome", async () => {
      recordNotebookCellExecution("malloy", "query", "ok", 100);
      recordNotebookCellExecution("malloy", "definition", "ok", 2);
      recordNotebookCellExecution("malloynb", "code", "denied", 40);

      expect(
         await harness.collectCounter(
            "publisher_notebook_cell_executions_total",
            { format: "malloy", kind: "query", outcome: "ok" },
         ),
      ).toBe(1);
      expect(
         await harness.collectCounter(
            "publisher_notebook_cell_executions_total",
            { format: "malloynb", kind: "code", outcome: "denied" },
         ),
      ).toBe(1);
      const ok = await harness.collectHistogram(
         "publisher_notebook_cell_execution_duration_ms",
         { format: "malloy", outcome: "ok" },
      );
      expect(ok.count).toBe(2);
      expect(ok.sum).toBe(102);
      // A slow warehouse query must land in a bucket, not in +Inf past 10s.
      expect(ok.boundaries).toContain(60000);
   });
});
