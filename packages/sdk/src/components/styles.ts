import { Card, CardContent, CardMedia, styled } from "@mui/material";

/**
 * Malloy brand colors — exact hex values from
 * `publisher/packages/app/public/logo.svg`. Use these instead of hardcoding
 * the hex values inline so the brand can be retuned in one place.
 */
export const MALLOY_BRAND = {
   teal: "#14b3cb", // light wing of the M
   orange: "#e47404", // right wing of the M
   darkBlue: "#1474a4", // deep shadow of the M
} as const;

/**
 * Accents that extend the logo's three, for the places that need more colors
 * than the brand supplies. The package page gives every kind of content its
 * own, and three kinds became six.
 *
 * Their own family rather than borrowings from the chart series
 * (`theme/defaults.ts`), because the two have opposite constraints. A series
 * color fills a large area and can be light; these sit behind a white icon as a
 * small solid backplate, where the icon has to stay legible. Measured against
 * white: violet 5.5:1, magenta 4.6:1, moss 3.9:1, so all three clear 3:1, the
 * WCAG minimum for a graphical object. The nearest series hues do not: `#aacd85`
 * sage is 1.8:1, `#ec72b8` pink 2.7:1, `#b87ced` violet 2.9:1.
 *
 * The bar is stated for these three only. `MALLOY_BRAND` above is inherited from
 * the logo and teal is 2.5:1, which is below it, so the row it backs has a
 * white-on-teal glyph that does not meet the same standard. Pre-existing, not
 * introduced here, and it is a brand decision rather than a component one.
 */
export const MALLOY_ACCENT = {
   violet: "#7c4dcc",
   magenta: "#c2478f",
   moss: "#5c8f3f",
} as const;

/**
 * Monospace font stack used by code-like surfaces inside the SDK
 * (file-path labels in PackageItemRow, code blocks, etc.). Matches the
 * `MONO_FONT_FAMILY` defined in the publisher app's theme.
 */
export const MONO_FONT_FAMILY =
   '"JetBrains Mono", "ui-monospace", "SFMono-Regular", "Menlo", monospace';

export const StyledCard = styled(Card)({
   display: "flex",
   flexDirection: "column",
   height: "100%",
   boxShadow: "none",
   border: "none",
   backgroundColor: "transparent",
});

export const StyledCardContent = styled(CardContent)({
   display: "flex",
   flexDirection: "column",
   padding: "0",
   flexGrow: 1,
});

export const StyledCardMedia = styled(CardMedia)({
   padding: "0",
});

// New clean notebook styles
export const CleanNotebookContainer = styled("div")(({ theme }) => ({
   backgroundColor: theme.palette.background.default,
   padding: "0 8px 0px 8px",
   borderRadius: "12px",
   boxShadow: "none",
   border: "none",
   maxWidth: "1200px",
   margin: "0 auto",
}));

export const CleanNotebookHeader = styled("div")(({ theme }) => ({
   marginBottom: "40px",
   paddingBottom: "24px",
   borderBottom: `1px solid ${theme.palette.divider}`,
}));

export const CleanNotebookSection = styled("div")({
   marginBottom: "48px",
   padding: "0",
   backgroundColor: "transparent",
   border: "none",
   boxShadow: "none",
});

export const CleanNotebookCell = styled("div")({
   marginBottom: "0",
   padding: "0",
   backgroundColor: "transparent",
   border: "none",
   boxShadow: "none",
});

export const CleanMetricCard = styled("div")({
   backgroundColor: "transparent",
   paddingTop: "12px",
   paddingBottom: "2px",
   borderRadius: "8px",
   border: "none",
   boxShadow: "none",
   marginBottom: "0",
});

export const CleanCodeBlock = styled("div")(({ theme }) => ({
   backgroundColor: theme.palette.mode === "dark" ? "#1e293b" : "#f8f9fa",
   padding: "16px",
   borderRadius: "8px",
   border: `1px solid ${theme.palette.divider}`,
   fontFamily: "'Monaco', 'Menlo', 'Ubuntu Mono', monospace",
   fontSize: "13px",
   lineHeight: "1.5",
   overflowX: "auto",
   color: theme.palette.text.primary,
}));

export const CleanActionBar = styled("div")(({ theme }) => ({
   backgroundColor: theme.palette.mode === "dark" ? "#1e293b" : "#f8f9fa",
   padding: "12px 16px",
   borderRadius: "8px",
   border: `1px solid ${theme.palette.divider}`,
   marginBottom: "16px",
   display: "flex",
   justifyContent: "space-between",
   alignItems: "center",
}));

export const StyledExplorerPage = styled("div")({
   height: "100%",
});

export const StyledExplorerBanner = styled("div")({
   height: "30px",
   backgroundColor: "rgba(225, 240, 255, 1)",
   display: "flex",
   padding: "4px",
   alignItems: "center",
});

export const StyledExplorerContent = styled("div")({
   height: "75vh",
   width: "100%",
   overflowY: "auto",
});

export const StyledExplorerPanel = styled("div")({
   position: "relative",
   height: "100%",
   flex: "0 0 auto",
});

// Package page styles
export const PackageCard = styled(Card)(({ theme }) => ({
   backgroundColor: theme.palette.background.paper,
   padding: "24px",
   borderRadius: "8px",
   border: `1px solid ${theme.palette.divider}`,
   boxShadow: "0 1px 3px rgba(0, 0, 0, 0.04)",
   height: "100%",
   transition: "box-shadow 0.2s ease-in-out",
   "&:hover": {
      boxShadow: "0 2px 6px rgba(0, 0, 0, 0.08)",
   },
}));

export const PackageCardContent = styled(CardContent)({
   padding: "0",
   "&:last-child": {
      paddingBottom: "0",
   },
});

export const PackageSectionTitle = styled("div")(({ theme }) => ({
   fontSize: "0.875rem",
   fontWeight: 500,
   color: theme.palette.text.secondary,
   marginBottom: "16px",
   paddingBottom: "8px",
   borderBottom: `1px solid ${theme.palette.divider}`,
}));

export const PackageContainer = styled("div")({
   padding: "32px",
   maxWidth: "1400px",
   margin: "0 auto",
   minHeight: "100vh",
});
