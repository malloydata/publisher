import * as React from "react";
import { styled } from "@mui/material/styles";
import Typography from "@mui/material/Typography";
import Breadcrumbs, { breadcrumbsClasses } from "@mui/material/Breadcrumbs";
import NavigateNextRoundedIcon from "@mui/icons-material/NavigateNextRounded";

const StyledBreadcrumbs = styled(Breadcrumbs)(({ theme }) => ({
  margin: theme.spacing(1, 0),
  [`& .${breadcrumbsClasses.separator}`]: {
    color: theme.palette.action.disabled,
    margin: 1,
  },
  [`& .${breadcrumbsClasses.ol}`]: {
    alignItems: "center",
  },
}));

export default function NavbarBreadcrumbs({
  selectedView,
}: {
  selectedView: "storefront" | "singleEmbed" | "dynamicDashboard" | "interactive";
}) {
  // Map key to display name
  const viewLabels: Record<typeof selectedView, string> = {
    storefront: "Storefront dashboard",
    singleEmbed: "Single Embed",
    dynamicDashboard: "Dynamic Dashboard",
    interactive: "Interactive",
  };

  return (
    <StyledBreadcrumbs
      aria-label="breadcrumb"
      separator={<NavigateNextRoundedIcon fontSize="small" />}
    >
      <Typography variant="body1">Dashboard</Typography>
      <Typography
        variant="body1"
        sx={{ color: "text.primary", fontWeight: 600 }}
      >
        {viewLabels[selectedView]}
      </Typography>
    </StyledBreadcrumbs>
  );
}
