// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Compare two main-thread CPU profiles (`.cpuprofile`, as `--cpu-prof` writes
 * them): the functions whose inclusive time grew most from the first to the
 * second, then each side's top self time.
 *
 *   bun tests/perf/main-thread/prof.ts <baseline.cpuprofile> <candidate.cpuprofile> [--top 25]
 *
 * A function's inclusive time counts each sample once even when the function
 * recurses, so a deep walk is not counted once per frame.
 */

interface ProfileNode {
   id: number;
   callFrame: { functionName: string; url: string; lineNumber: number };
   children?: number[];
}

interface Profile {
   nodes: ProfileNode[];
   samples: number[];
   timeDeltas: number[];
   startTime: number;
   endTime: number;
}

export interface ProfileTotals {
   totalMs: number;
   self: Map<string, number>;
   inclusive: Map<string, number>;
}

function nameOf(n: ProfileNode): string {
   const fn = n.callFrame.functionName || "(anonymous)";
   const file = n.callFrame.url.split("/").pop() ?? "";
   return file ? `${fn} (${file})` : fn;
}

export function totals(p: Profile): ProfileTotals {
   const byId = new Map(p.nodes.map((n) => [n.id, n]));
   const parent = new Map<number, number>();
   for (const n of p.nodes)
      for (const c of n.children ?? []) parent.set(c, n.id);
   const self = new Map<string, number>();
   const inclusive = new Map<string, number>();
   let totalMs = 0;
   for (let i = 0; i < p.samples.length; i++) {
      const ms = (p.timeDeltas[i + 1] ?? 0) / 1000;
      const leaf = byId.get(p.samples[i]);
      if (!leaf) continue;
      const leafName = nameOf(leaf);
      if (leafName.startsWith("(idle)")) continue;
      totalMs += ms;
      self.set(leafName, (self.get(leafName) ?? 0) + ms);
      const seen = new Set<string>();
      for (
         let id: number | undefined = leaf.id;
         id !== undefined;
         id = parent.get(id)
      ) {
         const name = nameOf(byId.get(id)!);
         if (seen.has(name)) continue;
         seen.add(name);
         inclusive.set(name, (inclusive.get(name) ?? 0) + ms);
      }
   }
   return { totalMs, self, inclusive };
}

if (import.meta.main) {
   const argv = process.argv.slice(2);
   const i = argv.indexOf("--top");
   const top = i >= 0 ? Number(argv[i + 1]) : 25;
   const files = argv.filter((a, k) => !a.startsWith("--") && k !== i + 1);
   if (files.length !== 2) {
      console.error(
         "usage: bun prof.ts <baseline.cpuprofile> <candidate.cpuprofile> [--top N]",
      );
      process.exit(2);
   }
   const [a, b] = await Promise.all(
      files.map(async (f) => totals((await Bun.file(f).json()) as Profile)),
   );
   const fmt = (n: number) => `${Math.round(n)}`.padStart(8);
   console.log(
      `busy main-thread time: baseline ${fmt(a.totalMs)} ms, candidate ${fmt(b.totalMs)} ms\n`,
   );
   console.log("grew most (inclusive ms): baseline -> candidate");
   const names = new Set([...a.inclusive.keys(), ...b.inclusive.keys()]);
   [...names]
      .map((n) => ({
         n,
         d: (b.inclusive.get(n) ?? 0) - (a.inclusive.get(n) ?? 0),
      }))
      .sort((x, y) => y.d - x.d)
      .slice(0, top)
      .forEach(({ n, d }) =>
         console.log(
            `${fmt(a.inclusive.get(n) ?? 0)} -> ${fmt(b.inclusive.get(n) ?? 0)}  (+${Math.round(d)})  ${n}`,
         ),
      );
   for (const [label, t] of [
      ["baseline", a],
      ["candidate", b],
   ] as const) {
      console.log(`\n${label}: top self time (ms)`);
      [...t.self.entries()]
         .sort((x, y) => y[1] - x[1])
         .slice(0, Math.min(top, 12))
         .forEach(([n, ms]) => console.log(`${fmt(ms)}  ${n}`));
   }
}
