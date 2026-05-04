export const colors = {
   white: "#FFFFFF",
   offWhite: "#F8F8F6",
   grey: {
      light: "#E6E4E1",
      mid: "#91908F",
   },
   black: "#30302E",

   accent: {
      sage: "#7F9862",
      brown: "#987362",
      olive: "#988962",
      steel: "#628698",
   },

   semantic: {
      success: { main: "#7A9461", light: "#E8EDDF", dark: "#5F7A4B" },
      warning: { main: "#B5943A", light: "#F3ECDA", dark: "#8E7430" },
      error: { main: "#BF6050", light: "#F2DDD9", dark: "#9C4D40" },
      info: { main: "#628698", light: "#DDE6EA", dark: "#4D6B79" },
   },
} as const;

export const contentTypeColors = {
   report: colors.accent.sage,
   model: colors.accent.steel,
   conversation: colors.accent.brown,
   dashboard: colors.accent.olive,
} as const;

export const MONO_FONT_FAMILY = '"Diatype Mono", monospace';
export const SANS_FONT_FAMILY =
   '"Diatype", "Helvetica Neue", "Arial", sans-serif';

export const greyScale = {
   50: colors.offWhite,
   100: "#F4F3F1",
   200: colors.grey.light,
   300: "#D4D2CF",
   400: "#B3B1AF",
   500: colors.grey.mid,
   600: "#73726F",
   700: "#555450",
   800: "#424140",
   900: colors.black,
} as const;
