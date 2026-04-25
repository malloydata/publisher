import {
   ArrowBack as BackIcon,
   Home as HomeIcon,
   Refresh as RefreshIcon,
} from "@mui/icons-material";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import {
   isRouteErrorResponse,
   useNavigate,
   useRouteError,
} from "react-router-dom";

function RouteError() {
   const error = useRouteError();
   const navigate = useNavigate();

   // Default values
   let title = "Unexpected Error";
   let message = "Something went wrong while loading this page.";

   // Parse error type
   if (isRouteErrorResponse(error)) {
      title = `Error ${error.status}`;
      message = error.statusText || error.data?.message || message;

      // Better messages for common status codes
      if (error.status === 404) {
         title = "Page Not Found";
         message = "The page you're looking for doesn't exist.";
      } else if (error.status === 403) {
         title = "Access Denied";
         message = "You don't have permission to access this resource.";
      } else if (error.status === 500) {
         title = "Server Error";
         message = "We're experiencing technical difficulties.";
      }
   } else if (error instanceof Error) {
      message = error.message;
   }

   const handleGoHome = () => navigate("/");
   const handleGoBack = () => window.history.back();
   const handleRefresh = () => window.location.reload();

   return (
      <Box
         display="flex"
         flexDirection="column"
         justifyContent="center"
         alignItems="center"
         minHeight="100vh"
         px={2}
      >
         <Stack
            spacing={3}
            alignItems="center"
            maxWidth={600}
            textAlign="center"
         >
            {/* Error Image */}
            <img src="/error.png" />

            {/* Error Title */}
            <Typography variant="h4" fontWeight={600}>
               {title}
            </Typography>

            {/* Error Message */}
            <Typography variant="body1" color="text.secondary">
               {message}
            </Typography>

            {/* Action Buttons */}
            <Stack
               direction={{ xs: "column", sm: "row" }}
               spacing={2}
               width="100%"
               maxWidth={400}
            >
               <Button
                  variant="contained"
                  startIcon={<HomeIcon />}
                  onClick={handleGoHome}
                  fullWidth
               >
                  Go Home
               </Button>
               <Button
                  variant="outlined"
                  startIcon={<BackIcon />}
                  onClick={handleGoBack}
                  fullWidth
               >
                  Go Back
               </Button>
               <Button
                  variant="outlined"
                  startIcon={<RefreshIcon />}
                  onClick={handleRefresh}
                  fullWidth
               >
                  Refresh
               </Button>
            </Stack>
         </Stack>
      </Box>
   );
}

export default RouteError;
