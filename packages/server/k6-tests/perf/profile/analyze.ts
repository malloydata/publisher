// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Summarize a .cpuprofile (from hook.mjs): self and total time per package,
// per source file and per function. Lines in the bundled dist/*.mjs map back to
// their src/ module through the bundler's "// src/..." comments, so a frame
// reads `validateRenderTags @ src/service/model.ts:<bundle line>` (the line is
// the bundle's; search the function name in the src file).
//
//   bun perf/profile/analyze.ts <file.cpuprofile> [topN] [--focus substr] [--callers fnName]
//
// --focus keeps only samples whose stack contains a frame matching substr.
// --callers lists the five frames above each call to fnName, weighted by time.
// Time is samples x the median sampling interval; idle time is not sampled.
import { readFileSync } from "fs";

const file = process.argv[2]!;
const topN = Number(process.argv[3] ?? 30);
const focusIdx = process.argv.indexOf("--focus");
const focus = focusIdx > 0 ? process.argv[focusIdx + 1] : undefined;
const callersIdx = process.argv.indexOf("--callers");
const callersOf = callersIdx > 0 ? process.argv[callersIdx + 1] : undefined;
const p = JSON.parse(readFileSync(file, "utf8"));

const bundleMaps = new Map<string, { line: number; mod: string }[]>();
function bundleModule(url: string, line: number): string | undefined {
   if (!/\/dist\/[^/]+\.mjs$/.test(url)) return undefined;
   let marks = bundleMaps.get(url);
   if (!marks) {
      marks = [];
      const lines = readFileSync(url.replace(/^file:\/\//, ""), "utf8").split(
         "\n",
      );
      lines.forEach((l, i) => {
         const m = /^\/\/ (src\/\S+|node_modules\/\S+|\.\.\/\S+)$/.exec(l);
         if (m) marks!.push({ line: i, mod: m[1]! });
      });
      bundleMaps.set(url, marks);
   }
   let lo = 0,
      hi = marks.length - 1,
      ans: string | undefined;
   while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (marks[mid]!.line <= line) {
         ans = marks[mid]!.mod;
         lo = mid + 1;
      } else hi = mid - 1;
   }
   return ans;
}
function where(cf: any): { file: string; pkg: string } {
   const url: string = cf.url || "";
   if (!url) return { file: "(native)", pkg: "(native)" };
   const bm = bundleModule(url, cf.lineNumber);
   if (bm) {
      const pkg = bm.startsWith("src/")
         ? "publisher:" + bm.split("/").slice(0, 2).join("/")
         : bm
              .replace(/^(\.\.\/)+/, "")
              .split("/")
              .slice(0, 3)
              .join("/");
      return { file: bm, pkg: bm.startsWith("src/") ? "publisher" : pkg };
   }
   const nm = url.lastIndexOf("node_modules/");
   if (nm >= 0) {
      const rest = url.slice(nm + 13).split("/");
      const pkg = rest[0]!.startsWith("@")
         ? rest.slice(0, 2).join("/")
         : rest[0]!;
      return { file: url.slice(nm + 13), pkg };
   }
   return {
      file: url.replace(/^file:\/\//, ""),
      pkg: url.includes("/dist/") ? "publisher" : "other",
   };
}

const nodes = new Map<number, any>();
for (const n of p.nodes) nodes.set(n.id, n);
const parent = new Map<number, number>();
for (const n of p.nodes) for (const c of n.children ?? []) parent.set(c, n.id);
const deltas: number[] = [...p.timeDeltas].sort(
   (a: number, b: number) => a - b,
);
const interval = (deltas[Math.floor(deltas.length / 2)] ?? 1000) / 1000; // ms
const counts = new Map<number, number>();
for (const s of p.samples) counts.set(s, (counts.get(s) ?? 0) + 1);
const totalSamples = p.samples.length;
const key = (n: any) => {
   const cf = n.callFrame;
   const w = where(cf);
   return `${cf.functionName || "(anonymous)"} @ ${w.file}:${cf.lineNumber + 1}`;
};
const selfBy = new Map<string, number>();
const totalBy = new Map<string, number>();
const selfFile = new Map<string, number>();
const selfPkg = new Map<string, number>();
const totalFile = new Map<string, number>();
const callerCounts = new Map<string, number>();
let focusSamples = 0;
for (const [id, c] of counts) {
   const leaf = nodes.get(id);
   const stack: any[] = [];
   for (
      let cur: number | undefined = id;
      cur !== undefined;
      cur = parent.get(cur)
   )
      stack.push(nodes.get(cur));
   if (focus && !stack.some((n) => key(n).includes(focus))) continue;
   focusSamples += c;
   const k = key(leaf);
   selfBy.set(k, (selfBy.get(k) ?? 0) + c);
   const w = where(leaf.callFrame);
   selfFile.set(w.file, (selfFile.get(w.file) ?? 0) + c);
   selfPkg.set(w.pkg, (selfPkg.get(w.pkg) ?? 0) + c);
   const seen = new Set<string>(),
      seenF = new Set<string>();
   for (const n of stack) {
      const kk = key(n);
      if (!seen.has(kk)) {
         seen.add(kk);
         totalBy.set(kk, (totalBy.get(kk) ?? 0) + c);
      }
      const f = where(n.callFrame).file;
      if (!seenF.has(f)) {
         seenF.add(f);
         totalFile.set(f, (totalFile.get(f) ?? 0) + c);
      }
   }
   if (callersOf) {
      for (let i = 0; i < stack.length; i++) {
         if ((stack[i].callFrame.functionName || "") === callersOf) {
            const chain = stack
               .slice(i + 1, i + 6)
               .map((n) => n.callFrame.functionName || "(anon)")
               .join(" < ");
            callerCounts.set(chain, (callerCounts.get(chain) ?? 0) + c);
            break;
         }
      }
   }
}
const ms = (c: number) => (c * interval).toFixed(0).padStart(7);
const pct = (c: number) =>
   ((100 * c) / focusSamples).toFixed(1).padStart(5) + "%";
const show = (title: string, m: Map<string, number>, n = topN) => {
   console.log(`\n## ${title}`);
   [...m.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, n)
      .forEach(([k, c]) => console.log(`${ms(c)} ms ${pct(c)}  ${k}`));
};
console.log(
   `# ${file}\nsamples=${totalSamples} interval≈${interval.toFixed(3)}ms sampled≈${(totalSamples * interval).toFixed(0)}ms` +
      (focus
         ? ` focus(${focus})=${focusSamples} (${(focusSamples * interval).toFixed(0)}ms)`
         : ""),
);
show("self by package", selfPkg, 15);
show("self by file", selfFile, 20);
show("total by file", totalFile, 25);
show("self by function", selfBy);
show("total by function", totalBy, topN + 20);
if (callersOf) show(`callers of ${callersOf}`, callerCounts, 15);
