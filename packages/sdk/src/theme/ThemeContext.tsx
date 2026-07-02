import React, {
   createContext,
   useContext,
   useMemo,
   type ReactNode,
} from "react";
import { resolveTheme } from "./resolveTheme";
import type { ResolvedTheme, Theme, ThemeMode } from "./types";

interface ThemeContextValue {
   /** Theme resolved from {@link layers} + {@link mode}. Used by the app shell. */
   theme: ResolvedTheme;
   /**
    * Raw base layers in cascade order. Today this holds only the
    * instance-level theme from `/status`; the built-in defaults live inside
    * {@link resolveTheme}, not in this array. Exposed so per-chart renderers
    * can append a per-chart override and re-run {@link resolveTheme} without
    * losing that instance baseline. The merge accepts any number of
    * lower-precedence layers, so an environment-level layer (already merged
    * server-side onto `Environment.theme`) can be fed in here later without
    * changing the merge.
    */
   layers: Array<Theme | undefined>;
   /**
    * The effective mode applied to charts and chrome (always "light" or "dark").
    * "auto" is resolved before reaching here.
    */
   mode: ThemeMode;
   /**
    * The viewer's stored preference: "light" | "dark" | "auto", or undefined
    * when no choice has been persisted. Use this (not {@link mode}) when
    * rendering a multi-state toggle that needs to expose "auto" as a position.
    */
   userChoice: ThemeMode | "auto" | undefined;
   setMode: (mode: ThemeMode | "auto") => void;
   allowUserToggle: boolean;
}

const ThemeContext = createContext<ThemeContextValue | undefined>(undefined);

interface ThemeProviderProps {
   layers: Array<Theme | undefined>;
   mode: ThemeMode;
   userChoice: ThemeMode | "auto" | undefined;
   setMode: (mode: ThemeMode | "auto") => void;
   allowUserToggle?: boolean;
   children: ReactNode;
}

export function ThemeProvider({
   layers,
   mode,
   userChoice,
   setMode,
   allowUserToggle = true,
   children,
}: ThemeProviderProps) {
   const theme = useMemo(() => resolveTheme(layers, mode), [layers, mode]);
   const value = useMemo<ThemeContextValue>(
      () => ({ theme, layers, mode, userChoice, setMode, allowUserToggle }),
      [theme, layers, mode, userChoice, setMode, allowUserToggle],
   );
   return (
      <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
   );
}

/**
 * Resolved publisher theme. Returns a sensible default outside a provider
 * so SDK consumers (third-party embedders, isolated stories) still get
 * styled output without having to mount the provider.
 */
export function usePublisherTheme(): ThemeContextValue {
   const ctx = useContext(ThemeContext);
   if (ctx) return ctx;
   return {
      theme: resolveTheme([], "light"),
      layers: [],
      mode: "light",
      userChoice: undefined,
      setMode: () => {},
      allowUserToggle: false,
   };
}
