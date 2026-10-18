// The file fails_first_start_worker.ts records its first start in: one per
// test process, shared by the test and the workers it starts.
import * as os from "node:os";
import * as path from "node:path";

export function failsFirstStartMarker(): string {
   return path.join(os.tmpdir(), `publisher-fails-first-start-${process.pid}`);
}
