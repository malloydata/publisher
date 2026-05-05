import { useServer, useRouterClickHandler } from "@malloy-publisher/sdk";
import ArticleOutlinedIcon from "@mui/icons-material/ArticleOutlined";
import CodeOutlinedIcon from "@mui/icons-material/CodeOutlined";
import FolderOutlinedIcon from "@mui/icons-material/FolderOutlined";
import HomeOutlinedIcon from "@mui/icons-material/HomeOutlined";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import SidebarToggleIcon from "./SidebarToggleIcon";
import {
   Box,
   IconButton,
   List,
   ListItemButton,
   ListItemIcon,
   ListItemText,
   Tooltip,
   Typography,
} from "@mui/material";
import { useQuery } from "@tanstack/react-query";
import { ReactElement, ReactNode } from "react";
import { useLocation, useParams } from "react-router-dom";

const SIDEBAR_WIDTH_EXPANDED = 260;
const SIDEBAR_WIDTH_COLLAPSED = 64;

interface SidebarProps {
   isCollapsed: boolean;
   onToggleCollapse: () => void;
   /** Optional override for the brand mark (passed through HeaderProps.logoHeader). */
   logoHeader?: ReactElement;
}

export default function Sidebar({
   isCollapsed,
   onToggleCollapse,
   logoHeader,
}: SidebarProps) {
   return (
      <Box
         sx={{
            height: "100dvh",
            width: isCollapsed
               ? SIDEBAR_WIDTH_COLLAPSED
               : SIDEBAR_WIDTH_EXPANDED,
            flexShrink: 0,
            display: "flex",
            flexDirection: "column",
            backgroundColor: "background.paper",
            transition: "width 0.2s cubic-bezier(0.4, 0, 0.2, 1)",
            overflow: "hidden",
         }}
      >
         <SidebarHeader
            isCollapsed={isCollapsed}
            onToggleCollapse={onToggleCollapse}
            logoHeader={logoHeader}
         />
         <Box sx={{ flex: 1, overflowY: "auto", overflowX: "hidden", py: 1 }}>
            <PrimaryNav isCollapsed={isCollapsed} />
            <ProjectsSection isCollapsed={isCollapsed} />
         </Box>
         <DocsFooter isCollapsed={isCollapsed} />
      </Box>
   );
}

function SidebarHeader({
   isCollapsed,
   onToggleCollapse,
   logoHeader,
}: {
   isCollapsed: boolean;
   onToggleCollapse: () => void;
   logoHeader?: ReactElement;
}) {
   const navigate = useRouterClickHandler();

   if (logoHeader) {
      return (
         <Box
            sx={{
               height: 56,
               display: "flex",
               alignItems: "center",
               justifyContent: "space-between",
               px: isCollapsed ? 0 : 2,
               flexShrink: 0,
            }}
         >
            {logoHeader}
            <IconButton
               size="small"
               onClick={onToggleCollapse}
               aria-label={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
            >
               <SidebarToggleIcon fontSize="small" />
            </IconButton>
         </Box>
      );
   }

   return (
      <Box
         sx={{
            height: 56,
            display: "flex",
            alignItems: "center",
            justifyContent: isCollapsed ? "center" : "space-between",
            px: isCollapsed ? 0 : 2,
            flexShrink: 0,
         }}
      >
         <Box
            onClick={(event) => navigate("/", event)}
            sx={{
               display: "flex",
               alignItems: "center",
               gap: 1,
               cursor: "pointer",
               minWidth: 0,
            }}
         >
            <Box
               component="img"
               src="/logo.svg"
               alt="Malloy"
               sx={{ width: 24, height: 24, flexShrink: 0 }}
            />
            {!isCollapsed && (
               <Typography
                  variant="subtitle1"
                  sx={{
                     color: "text.primary",
                     fontWeight: 500,
                     letterSpacing: "-0.025em",
                     whiteSpace: "nowrap",
                     overflow: "hidden",
                     textOverflow: "ellipsis",
                  }}
               >
                  Malloy Publisher
               </Typography>
            )}
         </Box>
         {!isCollapsed && (
            <IconButton
               size="small"
               onClick={onToggleCollapse}
               aria-label="Collapse sidebar"
            >
               <SidebarToggleIcon fontSize="small" />
            </IconButton>
         )}
      </Box>
   );
}

function PrimaryNav({ isCollapsed }: { isCollapsed: boolean }) {
   const location = useLocation();
   const isHome = location.pathname === "/";

   return (
      <List sx={{ py: 0 }}>
         <SidebarItem
            icon={<HomeOutlinedIcon fontSize="small" />}
            label="Home"
            to="/"
            selected={isHome}
            isCollapsed={isCollapsed}
         />
      </List>
   );
}

function ProjectsSection({ isCollapsed }: { isCollapsed: boolean }) {
   const { apiClients } = useServer();
   const params = useParams();

   // Match the SDK <Home /> query key so cache is shared across both surfaces.
   const { data: projects } = useQuery({
      queryKey: ["projects"],
      queryFn: () => apiClients.projects.listProjects(),
   });

   const list = projects?.data ?? [];

   if (list.length === 0) {
      return null;
   }

   return (
      <Box sx={{ mt: 1 }}>
         {!isCollapsed && (
            <Typography
               variant="caption"
               sx={{
                  display: "block",
                  px: 3,
                  py: 1,
                  color: "text.secondary",
                  fontWeight: 500,
                  textTransform: "uppercase",
                  fontSize: "0.6875rem",
                  letterSpacing: "0.5px",
               }}
            >
               Projects
            </Typography>
         )}
         <List sx={{ py: 0 }}>
            {list.map((project) => {
               const name = project.name ?? "";
               return (
                  <SidebarItem
                     key={name}
                     icon={<FolderOutlinedIcon fontSize="small" />}
                     label={name}
                     to={`/${name}`}
                     selected={params.projectName === name}
                     isCollapsed={isCollapsed}
                  />
               );
            })}
         </List>
      </Box>
   );
}

function DocsFooter({ isCollapsed }: { isCollapsed: boolean }) {
   const links = [
      {
         label: "Malloy Docs",
         href: "https://docs.malloydata.dev/documentation/",
         icon: <ArticleOutlinedIcon fontSize="small" />,
         external: true,
      },
      {
         label: "Publisher Docs",
         href: "https://github.com/malloydata/publisher/blob/main/README.md",
         icon: <OpenInNewIcon fontSize="small" />,
         external: true,
      },
      {
         label: "Publisher API",
         href: "/api-doc.html",
         icon: <CodeOutlinedIcon fontSize="small" />,
         external: false,
      },
   ];

   return (
      <List sx={{ py: 1 }}>
         {links.map((link) => (
            <ExternalLinkItem
               key={link.label}
               label={link.label}
               href={link.href}
               icon={link.icon}
               external={link.external}
               isCollapsed={isCollapsed}
            />
         ))}
      </List>
   );
}

function SidebarItem({
   icon,
   label,
   to,
   selected,
   isCollapsed,
}: {
   icon: ReactNode;
   label: string;
   to: string;
   selected: boolean;
   isCollapsed: boolean;
}) {
   const navigate = useRouterClickHandler();
   const inner = (
      <ListItemButton
         selected={selected}
         onClick={(event) => navigate(to, event)}
         sx={{
            justifyContent: isCollapsed ? "center" : "flex-start",
            px: isCollapsed ? 0 : 2,
         }}
      >
         <ListItemIcon
            sx={{
               minWidth: isCollapsed ? 0 : 36,
               justifyContent: "center",
               color: selected ? "text.primary" : "text.secondary",
            }}
         >
            {icon}
         </ListItemIcon>
         {!isCollapsed && (
            <ListItemText
               primary={label}
               primaryTypographyProps={{
                  variant: "body2",
                  sx: {
                     overflow: "hidden",
                     textOverflow: "ellipsis",
                     whiteSpace: "nowrap",
                  },
               }}
            />
         )}
      </ListItemButton>
   );

   if (isCollapsed) {
      return (
         <Tooltip title={label} placement="right">
            <Box>{inner}</Box>
         </Tooltip>
      );
   }
   return inner;
}

function ExternalLinkItem({
   label,
   href,
   icon,
   external,
   isCollapsed,
}: {
   label: string;
   href: string;
   icon: ReactNode;
   external: boolean;
   isCollapsed: boolean;
}) {
   const inner = (
      <ListItemButton
         component="a"
         href={href}
         target={external ? "_blank" : undefined}
         rel={external ? "noopener noreferrer" : undefined}
         sx={{
            justifyContent: isCollapsed ? "center" : "flex-start",
            px: isCollapsed ? 0 : 2,
         }}
      >
         <ListItemIcon
            sx={{
               minWidth: isCollapsed ? 0 : 36,
               justifyContent: "center",
               color: "text.secondary",
            }}
         >
            {icon}
         </ListItemIcon>
         {!isCollapsed && (
            <ListItemText
               primary={label}
               primaryTypographyProps={{
                  variant: "body2",
                  sx: {
                     color: "text.secondary",
                     overflow: "hidden",
                     textOverflow: "ellipsis",
                     whiteSpace: "nowrap",
                  },
               }}
            />
         )}
      </ListItemButton>
   );

   if (isCollapsed) {
      return (
         <Tooltip title={label} placement="right">
            <Box>{inner}</Box>
         </Tooltip>
      );
   }
   return inner;
}
