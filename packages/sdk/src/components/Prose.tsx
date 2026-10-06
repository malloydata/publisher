// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, type SxProps, type Theme } from "@mui/material";
import Markdown from "markdown-to-jsx";
import type { ReactNode } from "react";
import { usePublisherTheme } from "../theme/ThemeContext";
import type { NavigationClick } from "./click_helper";
import { MONO_FONT_FAMILY } from "./styles";

/**
 * Markdown authored inside a package — a dashboard's `##"` description, a
 * notebook's markdown cell, a README — rendered one way.
 *
 * Three surfaces each rendered it with their own style sheet, and only one of
 * them guarded the links. Package content comes from untrusted git and S3
 * sources, so a crafted `javascript:` or `data:` href in a README or a
 * description must never run; the notebook cell knew this and the other two did
 * not. Every link here goes through {@link ProseLink}.
 *
 * Two variants, because the prose plays two roles:
 *
 * - `document`: the prose IS the content — a notebook's markdown cells, read
 *   top to bottom. Full-size headings, reading-length lines.
 * - `caption`: the prose sits under a title it must not compete with — a
 *   dashboard's description, a package's README card. Body size; headings are
 *   section labels within the block, at body size and medium weight.
 */
export type ProseVariant = "document" | "caption";

/**
 * How a relative link is resolved. Links in package content are authored
 * relative to the SOURCE FILE (`spielberg.malloynb`), and a plain anchor
 * resolves them against the page URL instead, which drops the package segment
 * for a README shown at the package route. With this, a relative link resolves
 * against the file's own directory and routes through the host.
 */
export interface ProseLinkContext {
   environmentName: string;
   packageName: string;
   /** The file the prose came from, within the package; its directory is the base. */
   sourcePath?: string;
   /**
    * SPA navigation for a resolved link. Omitted, the absolute href navigates on
    * its own, so rendering prose needs no router context.
    */
   onNavigate?: (to: string, event?: NavigationClick) => void;
}

export interface ProseProps {
   children: string;
   variant?: ProseVariant;
   links?: ProseLinkContext;
   sx?: SxProps<Theme>;
}

const SAFE_SCHEME = /^(https?|mailto|tel):/i;
const HAS_SCHEME = /^[a-z][a-z0-9+.-]*:/i;

function ProseLink({
   href,
   title,
   children,
   context,
}: {
   href?: string;
   title?: string;
   children?: ReactNode;
   context: ProseLinkContext | undefined;
}) {
   const hasScheme = !!href && HAS_SCHEME.test(href);
   const isRelative =
      !!href && !hasScheme && !href.startsWith("/") && !href.startsWith("#");

   if (!href || !isRelative || !context) {
      // External, absolute, or hash: a normal anchor — but only over a safe
      // scheme, so a crafted href cannot run script.
      const unsafe = hasScheme && !SAFE_SCHEME.test(href ?? "");
      return (
         <a
            href={unsafe ? undefined : href}
            title={title}
            {...(hasScheme
               ? { target: "_blank", rel: "noopener noreferrer" }
               : {})}
         >
            {children}
         </a>
      );
   }

   // Resolve against the source file's directory (empty for a package-root
   // README) via a dummy origin, which normalises `./` and `../` and clamps
   // escapes at the package root, then prefix the package path.
   const sourceDir =
      context.sourcePath && context.sourcePath.includes("/")
         ? context.sourcePath.slice(0, context.sourcePath.lastIndexOf("/") + 1)
         : "";
   const resolved = new URL(href, `https://malloy.invalid/${sourceDir}`);
   const packageRelative =
      resolved.pathname.slice(1) + resolved.search + resolved.hash;
   const to = `/${context.environmentName}/${context.packageName}/${packageRelative}`;
   const { onNavigate } = context;
   return (
      <a
         href={to}
         title={title}
         onClick={
            onNavigate
               ? (event) => {
                    event.preventDefault();
                    onNavigate(to, event);
                 }
               : undefined
         }
      >
         {children}
      </a>
   );
}

const STYLES: Record<ProseVariant, SxProps<Theme>> = {
   document: {
      // Flush at both ends, as a caption is: the space around a block of
      // prose is the layout's (the gap between tiles), not its first and last
      // paragraph's, so a text tile's top lines up with a chart tile's title.
      // markdown-to-jsx wraps two or more blocks in a div, and returns one bare.
      "& > :first-child, & > div:only-child > :first-child": { mt: 0 },
      "& > :last-child, & > div:only-child > :last-child": { mb: 0 },
      "& h1, & h2, & h3, & h4, & h5, & h6": {
         fontWeight: "fontWeightMedium",
         color: "text.primary",
         mb: 1,
         mt: 2,
      },
      "& h1": { fontSize: "28px" },
      "& h2": { fontSize: "24px" },
      "& h3": { fontSize: "20px" },
      "& p, & ul, & ol": {
         color: "text.primary",
         lineHeight: 1.7,
         mb: 1,
         fontSize: "16px",
      },
      "& li": { mb: 0.5 },
   },
   caption: {
      color: "text.secondary",
      typography: "body2",
      // The block starts flush under whatever titles it and ends flush against
      // what follows, so a one-line caption sits where a plain line would and a
      // longer one grows downward rather than pushing its title around.
      "& > :first-of-type": { mt: 0 },
      "& > :last-child": { mb: 0 },
      // Headings in a caption are section labels within the block, not
      // competitors to the title above it, so they stay at body size.
      "& h1, & h2, & h3, & h4, & h5, & h6": {
         fontSize: "inherit",
         fontWeight: "fontWeightMedium",
         m: "0.5em 0 0.25em",
      },
      "& p": { m: "0.5em 0" },
      "& ul, & ol": { m: "0.5em 0", pl: 3 },
      "& code": { fontFamily: MONO_FONT_FAMILY, fontSize: "0.9em" },
   },
};

function ProseImageAlt({ alt }: { alt?: string }) {
   return <>{alt}</>;
}

export function Prose({
   children,
   variant = "caption",
   links,
   sx,
}: ProseProps) {
   // The instance theme's face, as a tile's heading takes it, so the prose
   // between tiles and the titles on them read as one typeface.
   const { theme } = usePublisherTheme();
   return (
      <Box
         sx={[
            STYLES[variant],
            // After the variant: `caption`'s `typography: body2` sets a face too.
            { fontFamily: theme.font.family },
            ...(Array.isArray(sx) ? sx : [sx]),
         ]}
      >
         <Markdown
            options={{
               // A document's single line is a paragraph like any other, so a
               // one-line text block reads with the same leading and spacing
               // as a longer one rather than as a bare inline run.
               forceBlock: variant === "document",
               // Authored text is not trusted markup: a `<meta refresh>`, `<iframe>`, `<form>` or `<style>` in it would act on the viewer's browser.
               disableParsingRawHTML: true,
               overrides: {
                  a: { component: ProseLink, props: { context: links } },
                  // `![](url)` would otherwise load from any server on view; the alt text is all that renders.
                  img: { component: ProseImageAlt },
               },
            }}
         >
            {children}
         </Markdown>
      </Box>
   );
}
