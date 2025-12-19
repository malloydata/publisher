import { Card, CardContent, CardMedia, styled } from "@mui/material";

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
export const CleanNotebookContainer = styled("div")({
   backgroundColor: "#ffffff",
   padding: "0 8px 0px 8px",
   borderRadius: "12px",
   boxShadow: "none",
   border: "none",
   maxWidth: "1200px",
   margin: "0 auto",
});

export const CleanNotebookHeader = styled("div")({
   marginBottom: "40px",
   paddingBottom: "24px",
   borderBottom: "1px solid #f0f0f0",
});

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
   backgroundColor: "#ffffff",
   paddingTop: "12px",
   paddingBottom: "2px",
   borderRadius: "8px",
   border: "1px solid #f0f0f0",
   boxShadow: "0 1px 3px rgba(0, 0, 0, 0.04)",
   marginBottom: "0",
   transition: "box-shadow 0.2s ease-in-out",
   "&:hover": {
      boxShadow: "0 2px 6px rgba(0, 0, 0, 0.08)",
   },
});

export const CleanCodeBlock = styled("div")({
   backgroundColor: "#f8f9fa",
   padding: "16px",
   borderRadius: "8px",
   border: "1px solid #e9ecef",
   fontFamily: "'Monaco', 'Menlo', 'Ubuntu Mono', monospace",
   fontSize: "13px",
   lineHeight: "1.5",
   overflowX: "auto",
});

export const CleanActionBar = styled("div")({
   backgroundColor: "#f8f9fa",
   padding: "12px 16px",
   borderRadius: "8px",
   border: "1px solid #e9ecef",
   marginBottom: "16px",
   display: "flex",
   justifyContent: "space-between",
   alignItems: "center",
});

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
export const PackageCard = styled(Card)({
   backgroundColor: "#ffffff",
   padding: "24px",
   borderRadius: "8px",
   border: "1px solid #f0f0f0",
   boxShadow: "0 1px 3px rgba(0, 0, 0, 0.04)",
   height: "100%",
   transition: "box-shadow 0.2s ease-in-out",
   "&:hover": {
      boxShadow: "0 2px 6px rgba(0, 0, 0, 0.08)",
   },
});

export const PackageCardContent = styled(CardContent)({
   padding: "0",
   "&:last-child": {
      paddingBottom: "0",
   },
});

export const PackageSectionTitle = styled("div")({
   fontSize: "12px",
   fontWeight: "600",
   color: "#666666",
   textTransform: "uppercase",
   letterSpacing: "0.5px",
   marginBottom: "16px",
   paddingBottom: "8px",
   borderBottom: "1px solid #f0f0f0",
});

export const PackageContainer = styled("div")({
   padding: "32px",
   maxWidth: "1400px",
   margin: "0 auto",
   minHeight: "100vh",
});
