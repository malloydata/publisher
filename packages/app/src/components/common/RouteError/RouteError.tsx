import {
   useRouteError,
   isRouteErrorResponse,
   useNavigate,
} from "react-router-dom";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import Button from "@mui/material/Button";
import Stack from "@mui/material/Stack";
import Paper from "@mui/material/Paper";
import Divider from "@mui/material/Divider";
import {
   Home as HomeIcon,
   ArrowBack as BackIcon,
   Refresh as RefreshIcon,
} from "@mui/icons-material";

export function RouteError() {
   const error = useRouteError();
   const navigate = useNavigate();

   // Default values
   let statusCode = 500;
   let title = "Unexpected Error";
   let message = "Something went wrong while loading this page.";

   // Parse error type
   if (isRouteErrorResponse(error)) {
      statusCode = error.status;
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

   // Log error for debugging
   console.error("Route Error:", error);

   const handleGoHome = () => navigate("/");
   const handleGoBack = () => window.history.back();
   const handleRefresh = () => window.location.reload();

   const isDevelopment = import.meta.env.DEV;

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
            {/* Error Code */}
            <Typography
               variant="h1"
               sx={{
                  fontSize: { xs: "4rem", sm: "6rem" },
                  fontWeight: 700,
                  color: "error.main",
                  lineHeight: 1,
               }}
            >
               {statusCode}
            </Typography>

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

            {/* Development Error Details */}
            {isDevelopment && (
               <>
                  <Divider sx={{ width: "100%", my: 2 }} />
                  <Paper
                     elevation={0}
                     sx={{
                        width: "100%",
                        p: 2,
                        bgcolor: "grey.100",
                        borderRadius: 1,
                        textAlign: "left",
                     }}
                  >
                     <Typography
                        variant="caption"
                        sx={{
                           fontWeight: 600,
                           color: "error.main",
                           textTransform: "uppercase",
                        }}
                     >
                        Development Error Details
                     </Typography>
                     <Typography
                        component="pre"
                        variant="caption"
                        sx={{
                           mt: 1,
                           whiteSpace: "pre-wrap",
                           wordBreak: "break-word",
                           fontFamily: "monospace",
                           fontSize: "0.75rem",
                           maxHeight: 300,
                           overflow: "auto",
                        }}
                     >
                        {error instanceof Error ? error.message : String(error)}
                        {"\n\n"}
                        {error instanceof Error && error.stack}
                     </Typography>
                  </Paper>
               </>
            )}
         </Stack>
      </Box>
   );
}
