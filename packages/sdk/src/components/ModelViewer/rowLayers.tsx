// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useState, type ReactNode } from "react";
import {
   IconCode,
   IconFields,
   IconGivens,
   IconLock,
   IconResult,
   IconValues,
} from "./icons";
import { SegmentIcons } from "./SegmentIcons";

/**
 * The kinds of content an open model row can show. An open row shows ONE card;
 * where it has more than one of these, the switch on its line picks which, in
 * this order.
 */
export type RowLayer =
   | "result"
   | "fields"
   | "code"
   | "values"
   | "givens"
   | "access";

const LAYER_ORDER: ReadonlyArray<RowLayer> = [
   "result",
   // Beside Result: the values a result ran with.
   "givens",
   "fields",
   "code",
   "values",
   "access",
];

/** Each layer's icon and word: a result's chart, the code brackets, the lock a
 *  governed thing carries. */
const LAYER_OPTIONS: Record<RowLayer, { icon: ReactNode; label: string }> = {
   result: { icon: <IconResult size={14} />, label: "Result" },
   fields: { icon: <IconFields size={14} />, label: "Fields" },
   code: { icon: <IconCode size={14} />, label: "Code" },
   values: { icon: <IconValues size={14} />, label: "Values" },
   givens: { icon: <IconGivens size={14} />, label: "Givens" },
   access: { icon: <IconLock size={14} />, label: "Access" },
};

/**
 * The layer an open row shows: the one the reader picked while it is available,
 * else `initial` (when available), else the first available. `available` is in
 * any order; it is read in {@link LAYER_ORDER}.
 */
export function useRowLayer(
   available: ReadonlyArray<RowLayer>,
   initial?: RowLayer,
) {
   const layers = LAYER_ORDER.filter((l) => available.includes(l));
   const [picked, setPicked] = useState<RowLayer | undefined>(undefined);
   const layer =
      picked !== undefined && layers.includes(picked)
         ? picked
         : initial !== undefined && layers.includes(initial)
           ? initial
           : layers[0];
   return { layers, layer, setLayer: setPicked };
}

/**
 * The switch between an open row's layers, drawn as action icons on its line.
 * Nothing when there is only one layer: a switch with one option has nothing to
 * switch. A layer the row has but cannot show here keeps its place, disabled with
 * its reason (SegmentIcons).
 */
export function RowLayerSwitch({
   layers,
   value,
   onChange,
   "aria-label": ariaLabel,
   disabledReasons,
}: {
   layers: ReadonlyArray<RowLayer>;
   value: RowLayer | undefined;
   onChange: (layer: RowLayer) => void;
   "aria-label": string;
   disabledReasons?: Partial<Record<RowLayer, string>>;
}) {
   if (layers.length < 2 || value === undefined) return null;
   return (
      <SegmentIcons<RowLayer>
         aria-label={ariaLabel}
         value={value}
         onChange={onChange}
         options={layers.map((l) => {
            const reason = disabledReasons?.[l];
            return {
               value: l,
               ...LAYER_OPTIONS[l],
               ...(reason ? { disabled: true, disabledReason: reason } : {}),
            };
         })}
      />
   );
}
