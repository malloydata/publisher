import Box from "@mui/material/Box";
import Container from "@mui/material/Container";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import { useRouteError } from "react-router-dom";

function RouteError() {
   const error = useRouteError();
   console.error(error);
   return (
      <Container
         maxWidth="lg"
         component="main"
         sx={{
            display: "flex",
            flexDirection: "column",
            my: 2,
            gap: 0,
         }}
      >
         <Stack
            sx={{
               m: "auto",
               flexDirection: "column",
            }}
         >
            <Box sx={{ height: "300px" }} />
            <img src="/error.png" />
            <Typography variant="subtitle1">
               An unexpected error occurred
            </Typography>
         </Stack>
      </Container>
   );
}
export default RouteError;
