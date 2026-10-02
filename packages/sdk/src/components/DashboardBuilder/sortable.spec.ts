// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PointerSensor } from "@dnd-kit/dom";
import { describe, expect, it } from "bun:test";
import { builderSensors } from "./sortable";

const preventActivation = PointerSensor.defaults.preventActivation as (
   event: PointerEvent,
   source: unknown,
) => boolean;

/** A press on `target` inside a draggable tile `card`. */
const press = (target: Element, card: Element) =>
   preventActivation({ target } as unknown as PointerEvent, {
      element: card,
      handle: undefined,
   });

describe("builderSensors", () => {
   it("starts a drag from anywhere on the tile", () => {
      const card = document.createElement("div");
      const configured = builderSensors[0] as unknown as {
         options: {
            activatorElements: (source: { element: Element }) => Element[];
         };
      };
      expect(configured.options.activatorElements({ element: card })).toEqual([
         card,
      ]);
   });

   const card = document.createElement("div");
   document.body.append(card);

   it("never starts a drag from the control that opens an editor", () => {
      // The display of an inline line is a button, the open editor a field.
      for (const tag of ["button", "input", "textarea"]) {
         const control = document.createElement(tag);
         const inner = document.createElement("span");
         control.append(inner);
         card.append(control);
         expect(press(control, card)).toBe(true);
         expect(press(inner, card)).toBe(true);
      }
   });

   it("still drags from the card itself", () => {
      expect(press(card, card)).toBe(false);
   });

   it("only starts a drag after a hold or some travel, so a click on the markdown display is a click", () => {
      const defaults = PointerSensor.defaults as unknown as {
         activationConstraints: (
            event: PointerEvent,
            source: unknown,
         ) => unknown[] | undefined;
      };
      const constraints = defaults.activationConstraints(
         { pointerType: "mouse", target: card } as unknown as PointerEvent,
         { handle: undefined },
      );
      expect(constraints?.length).toBeGreaterThan(0);
   });
});
