// The flow is markdown; these two tiny hooks are the ONE thing markdown can't do —
// capture an internal sourceEntityId and compare it across a rebuild. They share
// `api.state`, so the first stashes the id and the second asserts it changed.

import type { HookApi } from "../../../lib/scenario_md";
import type { Assert } from "../../framework";

export async function captureEid1(api: HookApi, assert: Assert): Promise<void> {
   const on = await api.use("on");
   const eid = (await on.sourceEntityIds("rmc"))["daily"];
   api.state.eid1 = eid;
   assert.ok("v1 has a content address", !!eid, JSON.stringify(eid));
}

export async function assertEidChanged(api: HookApi, assert: Assert): Promise<void> {
   const on = await api.use("on");
   const eid2 = (await on.sourceEntityIds("rmc"))["daily"];
   assert.ne(
      "model change ⇒ new content address (not skip-if-unchanged)",
      eid2,
      api.state.eid1,
   );
}
