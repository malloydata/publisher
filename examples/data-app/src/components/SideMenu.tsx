import * as React from "react";
import { styled } from "@mui/material/styles";
import Avatar from "@mui/material/Avatar";
import MuiDrawer, { drawerClasses } from "@mui/material/Drawer";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import MenuContent from "./MenuContent";
import { Tooltip } from "@mui/material";
import OptionsMenu from "./OptionsMenu";

const drawerWidth = 240;

const Drawer = styled(MuiDrawer)({
  width: drawerWidth,
  flexShrink: 0,
  boxSizing: "border-box",
  mt: 10,
  [`& .${drawerClasses.paper}`]: {
    width: drawerWidth,
    boxSizing: "border-box",
  },
});

export default function SideMenu({
  selectedView,
  setSelectedView,
}: {
  selectedView:
    | "storefront"
    | "singleEmbed"
    | "dynamicDashboard"
    | "interactive";
  setSelectedView: (
    view: "storefront" | "singleEmbed" | "dynamicDashboard" | "interactive"
  ) => void;
}) {
  const { user } = {
    user: { name: "John Doe", email: "john.doe@example.com" },
  };

  return (
    <Drawer
      variant="permanent"
      sx={{
        zIndex: 10,
        display: { xs: "none", md: "block" },
        [`& .${drawerClasses.paper}`]: {
          backgroundColor: "background.paper",
        },
      }}
    >
      <MenuContent
        selectedView={selectedView}
        setSelectedView={setSelectedView}
      />
      <Stack
        direction="row"
        sx={{
          p: 2,
          gap: 1,
          alignItems: "center",
          borderTop: "1px solid",
          borderColor: "divider",
        }}
      >
        <Avatar
          sizes="small"
          alt={user?.name}
          src="/static/images/avatar/7.jpg"
          sx={{ width: 36, height: 36 }}
        />
        <Box sx={{ mr: "auto", minWidth: 0 }}>
          <Typography
            variant="body2"
            sx={{ fontWeight: 500, lineHeight: "16px" }}
          >
            {user?.name}
          </Typography>
          <Tooltip title={user?.email || ""} placement="top">
            <Typography
              variant="caption"
              sx={{
                color: "text.secondary",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                display: "block",
                cursor: "default",
              }}
            >
              {user?.email}
            </Typography>
          </Tooltip>
        </Box>
        <OptionsMenu />
      </Stack>
    </Drawer>
  );
}
