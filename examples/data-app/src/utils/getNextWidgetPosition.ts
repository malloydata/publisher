// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Widget } from "../types/widget";

export const getNextWidgetPosition = (
  widgets: Widget[],
  widgetWidth: number,
  cols: number
) => {
  // Sort widgets by y then x
  const sorted = [...widgets].sort((a, b) => {
    if (a.layout.y === b.layout.y) {
      return a.layout.x - b.layout.x;
    }
    return a.layout.y - b.layout.y;
  });

  if (sorted.length === 0) {
    // First widget → top left
    return { x: 0, y: 0 };
  }

  const last = sorted[sorted.length - 1];

  // Attempt to place in same row if space allows
  const nextX = last.layout.x + last.layout.w;
  const nextY = last.layout.y;

  if (nextX + widgetWidth <= cols) {
    // There's room in current row → place next to last widget
    return { x: nextX, y: nextY };
  } else {
    // No room → place on next row
    return { x: 0, y: last.layout.y + last.layout.h };
  }
};
