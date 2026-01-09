import { Box } from "@mui/material";
import Container from "@mui/material/Container";
import { Outlet } from "react-router-dom";
import Header, { HeaderProps } from "../Header/Header";

interface PublisherConfigProps {
   headerProps?: HeaderProps;
}

export default function MainPage({ headerProps }: PublisherConfigProps) {
   return (
      <Box
         sx={{ display: "flex", flexDirection: "column", minHeight: "100vh" }}
      >
         <Header {...headerProps} />
         <Container
            maxWidth="xl"
            component="main"
            sx={{
               flex: 1,
               display: "flex",
               flexDirection: "column",
               py: 2,
               gap: 2,
            }}
         >
            {/* Page Content */}
            <Box sx={{ flex: 1 }}>
               <Outlet />
            </Box>
         </Container>
      </Box>
   );
}
