import MenuIcon from "@mui/icons-material/Menu";
import {
   AppBar,
   Box,
   Button,
   IconButton,
   Menu,
   MenuItem,
   Stack,
   Toolbar,
   Typography,
   useMediaQuery,
   useTheme,
} from "@mui/material";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import BreadcrumbNav from "./BreadcrumbNav";

export interface HeaderProps {
   logoHeader?: React.ReactElement;
   endCap?: React.ReactElement;
}

export default function Header({ logoHeader, endCap }: HeaderProps) {
   const navigate = useNavigate();
   const theme = useTheme();
   const isMobile = useMediaQuery(theme.breakpoints.down("sm"));
   const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
   const open = Boolean(anchorEl);

   const handleMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
      setAnchorEl(event.currentTarget);
   };
   const handleMenuClose = () => setAnchorEl(null);

   const menuItems = [
      {
         label: "Malloy Docs",
         link: "https://docs.malloydata.dev/documentation/",
         sx: { color: "#14b3cb" },
      },
      {
         label: "Publisher Docs",
         link: "https://github.com/malloydata/publisher/blob/main/README.md",
         sx: { color: "#14b3cb" },
      },
      {
         label: "Publisher API",
         link: "/api-doc.html",
         sx: { color: "#14b3cb" },
      },
   ];
   return (
      <AppBar
         position="sticky"
         elevation={0}
         sx={{
            backgroundColor: "background.paper",
            borderBottom: "1px solid",
            borderColor: "divider",
         }}
      >
         <Toolbar
            sx={{
               justifyContent: "space-between",
               flexWrap: "nowrap",
               minHeight: 64,
            }}
         >
            {logoHeader ? (
               logoHeader
            ) : (
               <Box
                  sx={{
                     display: "flex",
                     alignItems: "center",
                     gap: 1,
                     cursor: "pointer",
                  }}
                  onClick={() => navigate("/")}
               >
                  <Box
                     component="img"
                     src="/logo.svg"
                     alt="Malloy"
                     sx={{ width: 28, height: 28 }}
                  />
                  <Typography
                     variant="h6"
                     sx={{
                        color: "text.primary",
                        fontWeight: 700,
                        letterSpacing: "-0.025em",
                        fontSize: { xs: "1.1rem", sm: "1.25rem" },
                     }}
                  >
                     Malloy Publisher
                  </Typography>
               </Box>
            )}

            {isMobile ? (
               <>
                  <IconButton color="inherit" onClick={handleMenuOpen}>
                     <MenuIcon />
                  </IconButton>
                  <Menu
                     anchorEl={anchorEl}
                     open={open}
                     onClose={handleMenuClose}
                     anchorOrigin={{
                        vertical: "bottom",
                        horizontal: "right",
                     }}
                  >
                     {menuItems.map((item) => (
                        <MenuItem
                           key={item.label}
                           onClick={() => {
                              handleMenuClose();
                              window.location.href = item.link;
                           }}
                           sx={item.sx}
                        >
                           {item.label}
                        </MenuItem>
                     ))}
                     {endCap && <MenuItem>{endCap}</MenuItem>}
                  </Menu>
               </>
            ) : (
               <Stack direction="row" spacing={2} alignItems="center">
                  {menuItems.map((item) => (
                     <Button key={item.label} href={item.link} sx={item.sx}>
                        {item.label}
                     </Button>
                  ))}
                  {endCap}
               </Stack>
            )}
         </Toolbar>

         <Box
            sx={{
               borderTop: "1px solid",
               borderColor: "white",
               paddingTop: "0px",
               marginBottom: "1px",
               px: 2,
               py: 1,
               overflowX: "auto",
               bgcolor: "background.paper",
            }}
         >
            <BreadcrumbNav />
         </Box>
      </AppBar>
   );
}
