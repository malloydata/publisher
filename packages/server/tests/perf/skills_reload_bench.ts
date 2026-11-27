// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What loading a package that declares workflows and ships skill sets costs at
 * the caps, split by where it is paid. The walk and the hashing of every file
 * it reads run in the package-load worker, off the event loop; they count
 * against the load job's timeout. The main thread pays only to receive the
 * result (a structured-clone deserialize) and to adopt it: rebuild each
 * skill's reader and hash the shipped file shas into the package's source
 * hash. The main-thread rows are the time nothing else on the server runs.
 *
 *   bun tests/perf/skills_reload_bench.ts [--runs 5] [--sets 8]
 *
 * The cap package is built by tests/perf/skills_cap_package.ts. The first
 * call of each package is the first read; the rest are repeats over the same
 * files. The files were just written, so they are in the page cache: a box
 * reading from a cold disk pays more for the bytes.
 */

import { deserialize, serialize } from "bun:jsc";
import fs from "fs";
import os from "os";
import path from "path";
import { computeSourceContentSha } from "../../src/service/package_revision";
import {
   adoptWorkflows,
   readPackageWorkflows,
   workflowsToWire,
} from "../../src/service/skills/package_workflows";
import {
   CAP_SHAPE,
   writeCapPackage,
   writeFloorPackage,
   type Shape,
} from "./skills_cap_package";

function arg(name: string, fallback: string): string {
   const argv = process.argv;
   for (let i = 0; i < argv.length; i++) {
      if (argv[i] === `--${name}`) return argv[i + 1] ?? fallback;
      if (argv[i]?.startsWith(`--${name}=`)) {
         return argv[i]!.slice(name.length + 3);
      }
   }
   return fallback;
}

const RUNS = Number(arg("runs", "5"));
const SETS = Number(arg("sets", "8"));

const ms = (f: () => void): number => {
   const t = performance.now();
   f();
   return performance.now() - t;
};

const percentile = (values: number[], p: number): number => {
   const sorted = [...values].sort((a, b) => a - b);
   return sorted[
      Math.min(sorted.length - 1, Math.ceil(p * sorted.length) - 1)
   ]!;
};

function summarize(label: string, values: number[]): void {
   const p50 = percentile(values, 0.5).toFixed(1).padStart(7);
   const max = Math.max(...values)
      .toFixed(1)
      .padStart(7);
   const each = values.map((v) => v.toFixed(0)).join(", ");
   console.log(`${label.padEnd(28)} p50 ${p50} ms  max ${max} ms  (${each})`);
}

function measure(title: string, root: string, declared: unknown): void {
   const walks: number[] = [];
   const serializes: number[] = [];
   const deserializes: number[] = [];
   const adopts: number[] = [];
   const hashes: number[] = [];
   let first = 0;
   let read!: ReturnType<typeof readPackageWorkflows>;
   for (let i = 0; i <= RUNS; i++) {
      // In the worker: walk and hash every file the walk reads, then ship it.
      const walkMs = ms(() => (read = readPackageWorkflows(root, declared)));
      const wire = workflowsToWire(read);
      let bytes!: SharedArrayBuffer;
      const serializeMs = ms(() => (bytes = serialize(wire)));
      // On the main thread: receive it, rebuild the readers, hash the shas.
      let received!: typeof wire;
      const deserializeMs = ms(() => (received = deserialize(bytes)));
      let adopted!: ReturnType<typeof adoptWorkflows>;
      const adoptMs = ms(() => (adopted = adoptWorkflows(root, received)));
      const hashMs = ms(() =>
         computeSourceContentSha(adopted.contentShas, "declaration"),
      );
      if (i === 0) {
         first = walkMs;
         continue;
      }
      walks.push(walkMs);
      serializes.push(serializeMs);
      deserializes.push(deserializeMs);
      adopts.push(adoptMs);
      hashes.push(hashMs);
   }
   let listedFiles = 0;
   let listedBytes = 0;
   for (const set of read.sets.values()) {
      for (const skill of set.skills) {
         for (const a of skill.listed) {
            listedFiles++;
            listedBytes += a.bytes;
         }
      }
   }
   console.log(
      `\n${title}: ${read.workflows.size} workflows served over ${read.sets.size} sets, sets list ${listedFiles} files, ${(listedBytes / 1048576).toFixed(1)} MiB`,
   );
   console.log(`${"worker, first walk".padEnd(28)} ${first.toFixed(1)} ms`);
   summarize(`worker walk, ${RUNS} runs`, walks);
   summarize("worker serialize", serializes);
   summarize("main deserialize", deserializes);
   summarize("main adoptWorkflows", adopts);
   summarize("main computeSourceContentSha", hashes);
}

const SCENARIOS: Array<[string, Shape]> = [
   [`caps: ${SETS} sets and a ninth, every budget spent`, CAP_SHAPE],
   [
      "listed bytes only (no filler, 7 Markdown)",
      { walked: false, references: 0, tinyMarkdown: 0 },
   ],
   [
      "assets/ entries only (1,000 per skill)",
      { walked: true, references: 0, tinyMarkdown: 0 },
   ],
   [
      "references/ Markdown only (999 per skill)",
      { walked: false, references: 999, tinyMarkdown: 0 },
   ],
   [
      "reference/ Markdown only (907 per skill)",
      { walked: false, references: 0, tinyMarkdown: 900 },
   ],
];

const work = fs.mkdtempSync(path.join(os.tmpdir(), "skills-reload-bench-"));
try {
   const floor = path.join(work, "floor");
   measure("floor: one set, SKILL.md only", floor, writeFloorPackage(floor));
   for (const [title, shape] of SCENARIOS) {
      const root = path.join(work, "scenario");
      measure(title, root, writeCapPackage(root, SETS, shape));
      fs.rmSync(root, { recursive: true, force: true });
   }
} finally {
   fs.rmSync(work, { recursive: true, force: true });
}
