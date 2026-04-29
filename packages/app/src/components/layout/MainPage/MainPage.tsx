import { Loading } from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import { Suspense } from "react";
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
            <Box sx={{ flex: 1 }}>
               <Suspense fallback={<Loading />}>
                  <Outlet />
               </Suspense>
            </Box>
         </Container>
      </Box>
   );
}
