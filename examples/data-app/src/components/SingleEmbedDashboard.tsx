import { Stack, Typography, Box, Alert } from "@mui/material";
import Header from "./Header";
import { EmbeddedQueryResult } from "@malloy-publisher/sdk";
import { useState, useEffect } from "react";

export default function SingleEmbedDashboard({
  selectedView,
}: {
  selectedView: "malloySamples" | "singleEmbed" | "dynamicDashboard";
}) {
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  
  // TODO: Replace this embedded query with your own!
  // Copy the embedded query string from your dashboard or notebook
  // and paste it here. Make sure to keep the backticks and quotes intact.
  // Example format: {"modelPath":"your-model.malloynb","query":"your query here","optionalPackageName":"your-package","optionalProjectName":"your-project","resourceUri":"..."}
  const rawEmbeddedQuery = `{"modelPath":"names1.malloynb","query":"# line_chart\\nrun: names -> {\\n group_by: decade\\n  aggregate: total_population\\n  order_by: decade\\n}","optionalPackageName":"names","optionalProjectName":"malloy-samples","resourceUri":"publisher://projects/malloy-samples/packages/names/models/names1.malloynb"}`;
  
  // Process the query string the same way the UI does
  const embeddedQuery = rawEmbeddedQuery.replace(/\\n/g, '\n').replace(/\n/g, '\\n');
 
  useEffect(() => {
    // Validate the embedded query
    try {
      JSON.parse(embeddedQuery);
    } catch (parseError) {
      setError(`Failed to parse embedded query: ${parseError}`);
    }
    
    // Set loading to false after a short delay
    const timer = setTimeout(() => setIsLoading(false), 1000);
    return () => clearTimeout(timer);
  }, [embeddedQuery, rawEmbeddedQuery]);

  return (
    <Stack spacing={2} sx={{ mt: { xs: 8, md: 0 }, mb: 8 }}>
      <Header selectedView={selectedView} />
      
      <Box sx={{ p: 2 }}>
        <Typography variant="h5" sx={{ mb: 2, fontWeight: 'bold' }}>
          Single Embedded Query Result
        </Typography>
        <Typography variant="body1" sx={{ mb: 3, color: 'text.secondary' }}>
          Paste your embedded query string in the SingleEmbedDashboard.tsx file as the rawEmbeddedQuery variable above. Copy the query from your dashboard or notebook and replace the example below.
          <strong>Important:</strong> Keep the backticks (`) and quotes intact when pasting your query string. 
        </Typography>
        
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}
        
        {isLoading && (
          <Alert severity="info" sx={{ mb: 2 }}>
            Loading embedded query result...
          </Alert>
        )}
        
        <Box sx={{ 
          border: '1px solid #e0e0e0', 
          borderRadius: 2, 
          p: 2, 
          minHeight: 400, 
          backgroundColor: '#fafafa'
        }}>
          <Box
            sx={{
              width: "90%",
              height: "396px",
              overflow: "visible",
            }}
          >
            <EmbeddedQueryResult 
              embeddedQueryResult={embeddedQuery}
            />
          </Box>
        </Box>
      </Box>
    </Stack>
  );
}
