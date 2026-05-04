import Drawer from "@mui/material/Drawer";
import { ReactElement } from "react";
import Sidebar from "../Sidebar/Sidebar";

interface SideMenuMobileProps {
   open: boolean;
   onClose: () => void;
   logoHeader?: ReactElement;
}

export default function SideMenuMobile({
   open,
   onClose,
   logoHeader,
}: SideMenuMobileProps) {
   return (
      <Drawer
         anchor="left"
         open={open}
         onClose={onClose}
         ModalProps={{ keepMounted: true }}
         sx={{
            display: { xs: "block", md: "none" },
            "& .MuiDrawer-paper": {
               boxSizing: "border-box",
               width: 260,
            },
         }}
      >
         <Sidebar
            isCollapsed={false}
            onToggleCollapse={onClose}
            logoHeader={logoHeader}
         />
      </Drawer>
   );
}
