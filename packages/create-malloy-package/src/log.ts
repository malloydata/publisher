/**
 * Minimal ANSI coloring with no dependency. Color is disabled when stdout is not a
 * TTY or when NO_COLOR is set, so piped and CI output stays clean.
 */
const useColor = Boolean(process.stdout.isTTY) && !process.env.NO_COLOR;

function paint(code: string, text: string): string {
   return useColor ? `\x1b[${code}m${text}\x1b[0m` : text;
}

export const bold = (text: string): string => paint("1", text);
export const dim = (text: string): string => paint("2", text);
export const green = (text: string): string => paint("32", text);
export const cyan = (text: string): string => paint("36", text);
export const yellow = (text: string): string => paint("33", text);
export const red = (text: string): string => paint("31", text);
