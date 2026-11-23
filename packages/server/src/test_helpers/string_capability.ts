// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Model } from "../service/model";
import { refreshStringCapabilities } from "../service/materialization/serve/source_capability";

/**
 * Read how `model`'s sources compare strings, as every bind commit does, for
 * a spec that sets a model's bindings directly (`setServeBindings`) with no
 * bind: read admission's lookups judge its answers by it.
 */
export async function recordModelCapabilities(model: Model): Promise<void> {
   model.setStringCapabilityReader(refreshStringCapabilities);
   await model.refreshStringCapabilities();
}
