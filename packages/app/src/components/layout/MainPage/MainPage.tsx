import { Loading } from "@malloy-publisher/sdk";
import SidebarToggleIcon from "../Sidebar/SidebarToggleIcon";
import Box from "@mui/material/Box";
import IconButton from "@mui/material/IconButton";
import { useTheme } from "@mui/material/styles";
import useMediaQuery from "@mui/material/useMediaQuery";
import { Suspense, useEffect, useState } from "react";
import { Outlet } from "react-router-dom";
import { layout } from "../../../theme";
import { HeaderProps } from "../Header/Header";
import BreadcrumbNav from "../BreadcrumbNav/BreadcrumbNav";
import Sidebar from "../Sidebar/Sidebar";
import SideMenuMobile from "../SideMenuMobile/SideMenuMobile";

interface MainPageProps {
   headerProps?: HeaderProps;
}

export default function MainPage({ headerProps }: MainPageProps) {
   const theme = useTheme();
   const isMdUp = useMediaQuery(theme.breakpoints.up("md"));
   const [mobileOpen, setMobileOpen] = useState(false);
   const [collapsed, setCollapsed] = useState(false);

   useEffect(() => {
      if (isMdUp && mobileOpen) {
         setMobileOpen(false);
      }
   }, [isMdUp, mobileOpen]);

   return (
      <Box
         sx={{
            height: "100dvh",
            display: "flex",
            flexDirection: "row",
            bgcolor: "background.default",
         }}
      >
         {isMdUp && (
            <Sidebar
               isCollapsed={collapsed}
               onToggleCollapse={() => setCollapsed((prev) => !prev)}
               logoHeader={headerProps?.logoHeader}
            />
         )}

         <Box
            component="main"
            sx={{
               flex: 1,
               display: "flex",
               flexDirection: "column",
               minWidth: 0,
            }}
         >
            <Box
               sx={{
                  flexShrink: 0,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  height: layout.headerHeight,
                  px: { xs: 1, md: 3 },
                  backgroundColor: "background.default",
               }}
            >
               <Box
                  sx={{
                     display: { xs: "flex", md: "none" },
                     alignItems: "center",
                     mr: 1,
                  }}
               >
                  <IconButton
                     size="small"
                     onClick={() => setMobileOpen(true)}
                     aria-label="Open navigation"
                  >
                     <SidebarToggleIcon fontSize="small" />
                  </IconButton>
               </Box>

               <Box
                  sx={{
                     flex: 1,
                     minWidth: 0,
                     display: "flex",
                     alignItems: "center",
                  }}
               >
                  <BreadcrumbNav />
               </Box>

               <Box
                  id="header-actions-portal"
                  sx={{
                     display: "flex",
                     alignItems: "center",
                     flexShrink: 0,
                     gap: 1,
                  }}
               >
                  {headerProps?.endCap}
               </Box>
            </Box>

            <Box
               sx={{
                  flex: 1,
                  overflow: "auto",
                  minWidth: 320,
                  minHeight: 0,
               }}
            >
               <Suspense fallback={<Loading />}>
                  <Outlet />
               </Suspense>
            </Box>
         </Box>

         <SideMenuMobile
            open={mobileOpen}
            onClose={() => setMobileOpen(false)}
            logoHeader={headerProps?.logoHeader}
         />
      </Box>
   );
}
