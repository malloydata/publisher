import React, { useState } from 'react';
import { 
  Box, 
  Typography, 
  Card, 
  Stack, 
  Tabs, 
  Tab, 
  FormControl, 
  InputLabel, 
  Select, 
  MenuItem, 
  Paper,
  CircularProgress,
  Alert,
  SelectChangeEvent
} from "@mui/material";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
} from "recharts";
import Header from "./Header";

// Custom hook to fetch raw query data using the existing API
const useRawQueryData = ({ modelPath, query }: { modelPath: string; query: string }) => {
  const [data, setData] = React.useState<any[]>([]);
  const [isLoading, setIsLoading] = React.useState(false);
  const [isError, setIsError] = React.useState(false);

  React.useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      setIsError(false);
      try {
        // Using the names package which is DuckDB based and doesn't require BQ credentials
        const response = await fetch(`/api/v0/projects/malloy-samples/packages/names/models/${modelPath}/query`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ query })
        });

        if (!response.ok) throw new Error('Failed to fetch');
        
        const result = await response.json();
        const parsed = typeof result.result === 'string' ? JSON.parse(result.result) : result;
        
        // Convert the Malloy result format to simple objects
        const arrayData = parsed.data?.array_value || [];
        const processedData = arrayData.map((row: any) => {
          const record = row.record_value || [];
          const obj: any = {};
          
          // Map field values based on schema
          record.forEach((cell: any, index: number) => {
            const fieldName = parsed.schema?.fields[index]?.name;
            if (fieldName) {
              if (cell.string_value !== undefined) {
                obj[fieldName] = cell.string_value;
              } else if (cell.number_value !== undefined) {
                obj[fieldName] = cell.number_value;
              }
            }
          });
          
          return obj;
        });
        
        setData(processedData);
      } catch (error) {
        console.error('Failed to fetch query results:', error);
        setIsError(true);
        setData([]);
      } finally {
        setIsLoading(false);
      }
    };

    if (query) {
      fetchData();
    }
  }, [modelPath, query]);

  return { data, isLoading, isError };
};

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`simple-tabpanel-${index}`}
      aria-labelledby={`simple-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

const COLORS = {
  primary: '#3B82F6',
  secondary: '#10B981', 
  accent: '#F59E0B',
  purple: '#8B5CF6',
  pink: '#EC4899',
  chartColors: ['#3B82F6', '#10B981', '#F59E0B', '#8B5CF6', '#EC4899', '#6B7280', '#818CF8', '#F472B6']
};

export default function InteractiveDashboard({
  selectedView,
}: {
  selectedView: "malloySamples" | "singleEmbed" | "dynamicDashboard" | "interactive";
}) {
  const [tabValue, setTabValue] = useState(0);
  const [stateFilter, setStateFilter] = useState('');
  const [genderFilter, setGenderFilter] = useState('');
  const containerRef = React.useRef<HTMLDivElement>(null);

  // Store scroll position to prevent auto-scroll to top
  const scrollPositionRef = React.useRef(0);

  React.useEffect(() => {
    const handleScroll = () => {
      scrollPositionRef.current = window.pageYOffset;
    };

    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  const handleTabChange = React.useCallback((event: React.SyntheticEvent, newValue: number) => {
    event.preventDefault();
    event.stopPropagation();
    
    // Store current scroll position
    const currentScroll = window.pageYOffset;
    scrollPositionRef.current = currentScroll;
    
    setTabValue(newValue);
    
    // Prevent scroll to top by restoring position immediately
    requestAnimationFrame(() => {
      window.scrollTo(0, currentScroll);
    });
  }, []);

  const handleStateFilterChange = React.useCallback((event: SelectChangeEvent) => {
    event.preventDefault();
    
    // Store current scroll position
    const currentScroll = window.pageYOffset;
    scrollPositionRef.current = currentScroll;
    
    setStateFilter(event.target.value);
    
    // Prevent scroll to top by restoring position immediately
    requestAnimationFrame(() => {
      window.scrollTo(0, currentScroll);
    });
  }, []);

  const handleGenderFilterChange = React.useCallback((event: SelectChangeEvent) => {
    event.preventDefault();
    
    // Store current scroll position
    const currentScroll = window.pageYOffset;
    scrollPositionRef.current = currentScroll;
    
    setGenderFilter(event.target.value);
    
    // Prevent scroll to top by restoring position immediately
    requestAnimationFrame(() => {
      window.scrollTo(0, currentScroll);
    });
  }, []);

  // Build filtered query with where clause appended
  const buildFilteredQuery = (baseQuery: string) => {
    const conditions = [];
    if (stateFilter) {
      conditions.push(`state = '${stateFilter}'`);
    }
    if (genderFilter) {
      conditions.push(`gender = '${genderFilter}'`);
    }
    
    if (conditions.length > 0) {
      return `${baseQuery} + { where: ${conditions.join(' and ')} }`;
    }
    return baseQuery;
  };

  // Use names1.malloynb - the actual model file in the malloy-samples package
  const MODEL_PATH = 'names1.malloynb'; 

  // Queries for the dashboard
  const topNamesQuery = buildFilteredQuery(`run: names -> { 
    group_by: name
    aggregate: total_population
    limit: 10
  }`);
  
  const byStateQuery = buildFilteredQuery(`run: names -> { 
    group_by: state
    aggregate: total_population
    limit: 10 
  }`);
  
  const byGenderQuery = buildFilteredQuery(`run: names -> { 
    group_by: gender
    aggregate: total_population 
  }`);
  
  const namesByDecadeQuery = buildFilteredQuery(`run: names -> { 
    group_by: decade
    aggregate: total_population
    order_by: decade
  }`);

  const namesByStateGenderQuery = buildFilteredQuery(`run: names -> { 
    group_by: state, gender
    aggregate: total_population
    limit: 20
  }`);

  // Use the custom hook to fetch raw data
  const { data: topNamesData, isLoading: topNamesLoading, isError: topNamesError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: topNamesQuery,
  });

  const { data: byStateData, isLoading: byStateLoading, isError: byStateError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: byStateQuery,
  });

  const { data: byGenderData, isLoading: byGenderLoading, isError: byGenderError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: byGenderQuery,
  });

  const { data: byDecadeData, isLoading: byDecadeLoading, isError: byDecadeError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: namesByDecadeQuery,
  });

  const { data: namesByStateGenderData, isLoading: namesByStateGenderLoading, isError: namesByStateGenderError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: namesByStateGenderQuery,
  });

  // Process data for charts
  const processedTopNamesData = topNamesData?.map((item: any, index: number) => ({
    name: item.name || 'Unknown',
    count: Number(item.total_population) || 0,
    fill: COLORS.chartColors[index % COLORS.chartColors.length]
  })) || [];

  const processedByStateData = byStateData?.map((item: any, index: number) => ({
    name: item.state || 'Unknown',
    value: Number(item.total_population) || 0,
    fill: COLORS.chartColors[index % COLORS.chartColors.length]
  })) || [];

  const processedByGenderData = byGenderData?.map((item: any, index: number) => ({
    name: item.gender === 'F' ? 'Female' : item.gender === 'M' ? 'Male' : item.gender,
    count: Number(item.total_population) || 0,
    fill: item.gender === 'F' ? COLORS.pink : COLORS.primary
  })) || [];

  const processedByDecadeData = byDecadeData?.map((item: any) => ({
    decade: item.decade?.toString() || 'Unknown',
    'Population': Number(item.total_population) || 0
  })) || [];

  // Process mekko/stacked bar data
  const processedStackedData = React.useMemo(() => {
    if (!namesByStateGenderData || namesByStateGenderData.length === 0) return [];
    
    const stateMap: {[key: string]: any} = {};
    namesByStateGenderData.forEach((item: any) => {
      const state = item.state;
      if (!stateMap[state]) {
        stateMap[state] = { name: state, Male: 0, Female: 0 };
      }
      const gender = item.gender === 'M' ? 'Male' : 'Female';
      stateMap[state][gender] = Number(item.total_population);
    });
    
    return Object.values(stateMap).sort((a: any, b: any) => (b.Male + b.Female) - (a.Male + a.Female)).slice(0, 10);
  }, [namesByStateGenderData]);

  const tabs = [
    { id: 0, label: 'Top Names' },
    { id: 1, label: 'By State' },
    { id: 2, label: 'By Gender' },
    { id: 3, label: 'State & Gender' },
    { id: 4, label: 'Over Time' }
  ];

  // Calculate scale to fit content in viewport
  const [scale, setScale] = React.useState(1);
  
  React.useEffect(() => {
    const calculateScale = () => {
      const viewportHeight = window.innerHeight;
      const headerHeight = 100; // Approximate header height
      const availableHeight = viewportHeight - headerHeight;
      const contentHeight = 900; // Approximate content height (reduced)
      
      if (contentHeight > availableHeight) {
        const newScale = Math.max(0.7, Math.min(availableHeight / contentHeight, 1));
        setScale(newScale);
      } else {
        setScale(1);
      }
    };

    calculateScale();
    window.addEventListener('resize', calculateScale);
    return () => window.removeEventListener('resize', calculateScale);
  }, []);

  return (
    <Stack spacing={2} sx={{ mt: { xs: 8, md: 0 }, mb: 8 }}>
      <Header selectedView={selectedView} />
      
      <Box 
        ref={containerRef} 
        sx={{ 
          maxWidth: 1400, 
          mx: 'auto', 
          p: 3,
          scrollBehavior: 'smooth',
          transform: `scale(${scale})`,
          transformOrigin: 'top center',
          width: scale < 1 ? `${100 / scale}%` : '100%',
        }}
      >
        <Typography 
          variant="h4" 
          sx={{ 
            fontWeight: 'bold', 
            mb: 2, 
            background: 'linear-gradient(45deg, #1976d2, #42a5f5)',
            backgroundClip: 'text',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent',
          }}
        >
          US Name Popularity Explorer
        </Typography>
        <Typography variant="body1" sx={{ mb: 4, color: '#6b7280' }}>
          Explore historical US baby name trends (SSA dataset) with interactive filtering.
        </Typography>

        {/* Statistics Cards */}
        <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} sx={{ mb: 3 }}>
          <Card sx={{ 
            p: 3, 
            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
            color: 'white',
            boxShadow: '0 8px 32px rgba(102, 126, 234, 0.3)',
            flex: 1
          }}>
            <Typography variant="h4" sx={{ fontWeight: 'bold' }}>300M+</Typography>
            <Typography variant="body2">Total Population</Typography>
          </Card>
          <Card sx={{ 
            p: 3, 
            background: 'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)',
            color: 'white',
            boxShadow: '0 8px 32px rgba(240, 147, 251, 0.3)',
            flex: 1
          }}>
            <Typography variant="h4" sx={{ fontWeight: 'bold' }}>100K+</Typography>
            <Typography variant="body2">Unique Names</Typography>
          </Card>
          <Card sx={{ 
            p: 3, 
            background: 'linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)',
            color: 'white',
            boxShadow: '0 8px 32px rgba(79, 172, 254, 0.3)',
            flex: 1
          }}>
            <Typography variant="h4" sx={{ fontWeight: 'bold' }}>1910-2020</Typography>
            <Typography variant="body2">Years Covered</Typography>
          </Card>
        </Stack>

        {/* Interactive Filters - Above Tabs */}
        <Card sx={{ p: 2, mb: 2 }}>
          <Typography variant="h6" sx={{ mb: 2, fontWeight: 'bold' }}>
            Interactive Filters
          </Typography>
          <Typography variant="body2" sx={{ mb: 3, color: '#6b7280' }}>
            Apply filters to modify all charts below.
          </Typography>
          
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={3} sx={{ mb: 2 }}>
            <FormControl sx={{ minWidth: 200 }}>
              <InputLabel>Filter by State</InputLabel>
              <Select
                value={stateFilter}
                label="Filter by State"
                onChange={handleStateFilterChange}
              >
                <MenuItem value="">All States</MenuItem>
                <MenuItem value="CA">California</MenuItem>
                <MenuItem value="TX">Texas</MenuItem>
                <MenuItem value="NY">New York</MenuItem>
                <MenuItem value="FL">Florida</MenuItem>
                <MenuItem value="IL">Illinois</MenuItem>
                <MenuItem value="PA">Pennsylvania</MenuItem>
                <MenuItem value="OH">Ohio</MenuItem>
                <MenuItem value="MI">Michigan</MenuItem>
                <MenuItem value="GA">Georgia</MenuItem>
                <MenuItem value="NC">North Carolina</MenuItem>
              </Select>
            </FormControl>

            <FormControl sx={{ minWidth: 200 }}>
              <InputLabel>Filter by Gender</InputLabel>
              <Select
                value={genderFilter}
                label="Filter by Gender"
                onChange={handleGenderFilterChange}
              >
                <MenuItem value="">All Genders</MenuItem>
                <MenuItem value="F">Female</MenuItem>
                <MenuItem value="M">Male</MenuItem>
              </Select>
            </FormControl>
          </Stack>

          {(stateFilter || genderFilter) && (
            <Alert severity="info" sx={{ mt: 2 }}>
              <Typography variant="body2">
                <strong>Active Filters:</strong>{' '}
                {stateFilter && `State = "${stateFilter}"`}
                {stateFilter && genderFilter && ' AND '}
                {genderFilter && `Gender = "${genderFilter}"`}
              </Typography>
              <Typography variant="body2" sx={{ mt: 1, fontFamily: 'monospace', fontSize: '0.8rem' }}>
                Query modification: + where: {[
                  stateFilter && `state = '${stateFilter}'`,
                  genderFilter && `gender = '${genderFilter}'`
                ].filter(Boolean).join(' and ')}
              </Typography>
            </Alert>
          )}
        </Card>

        {/* Tab Navigation */}
        <Paper sx={{ mb: 2 }}>
          <Tabs 
            value={tabValue} 
            onChange={handleTabChange}
            sx={{
              '& .MuiTabs-indicator': {
                backgroundColor: '#1976d2',
              },
              '& .MuiTab-root': {
                textTransform: 'none',
                fontWeight: 500,
              },
            }}
          >
            {tabs.map((tab) => (
              <Tab key={tab.id} label={tab.label} />
            ))}
          </Tabs>
        </Paper>

        {/* Tab Content */}
        <TabPanel value={tabValue} index={0}>
          <Card sx={{ p: 3 }}>
            <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold' }}>
              Top 10 Most Popular Names
            </Typography>
            {topNamesLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : topNamesError ? (
              <Alert severity="error">Failed to load names data</Alert>
            ) : (
              <ResponsiveContainer width="100%" height={350}>
                <BarChart 
                  data={processedTopNamesData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 50 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis 
                    dataKey="name" 
                    stroke="#666" 
                  />
                  <YAxis stroke="#666" />
                  <Tooltip 
                    formatter={(value) => [value.toLocaleString(), 'Births']}
                  />
                  <Bar dataKey="count" fill="#3B82F6" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </Card>
        </TabPanel>

        <TabPanel value={tabValue} index={1}>
          <Card sx={{ p: 3 }}>
            <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold' }}>
              Population by State (Top 10)
            </Typography>
            {byStateLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : byStateError ? (
              <Alert severity="error">Failed to load state data</Alert>
            ) : (
              <ResponsiveContainer width="100%" height={350}>
                <BarChart 
                  data={processedByStateData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 50 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis dataKey="name" stroke="#666" />
                  <YAxis stroke="#666" />
                  <Tooltip 
                    formatter={(value) => [value.toLocaleString(), 'Births']}
                  />
                  <Bar dataKey="value" fill={COLORS.secondary} radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </Card>
        </TabPanel>

        <TabPanel value={tabValue} index={2}>
          <Card sx={{ p: 3 }}>
            <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold' }}>
              Distribution by Gender
            </Typography>
            {byGenderLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : byGenderError ? (
              <Alert severity="error">Failed to load gender data</Alert>
            ) : (
              <ResponsiveContainer width="100%" height={400}>
                <PieChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                  <Pie
                    data={processedByGenderData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={140}
                    dataKey="count"
                    label={({ name, count }) => `${name}: ${count.toLocaleString()}`}
                  >
                    {processedByGenderData.map((entry: any, index: number) => (
                      <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value) => [value.toLocaleString(), 'Births']} />
                </PieChart>
              </ResponsiveContainer>
            )}
          </Card>
        </TabPanel>

        <TabPanel value={tabValue} index={3}>
          <Card sx={{ p: 3 }}>
            <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold' }}>
              Births by State and Gender
            </Typography>
            {namesByStateGenderLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : namesByStateGenderError ? (
              <Alert severity="error">Failed to load stacked data</Alert>
            ) : (
              <ResponsiveContainer width="100%" height={400}>
                <BarChart
                  data={processedStackedData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 5 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="name" />
                  <YAxis />
                  <Tooltip formatter={(value) => [value.toLocaleString(), 'Births']} />
                  <Legend />
                  <Bar dataKey="Female" stackId="a" fill={COLORS.pink} />
                  <Bar dataKey="Male" stackId="a" fill={COLORS.primary} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </Card>
        </TabPanel>

        <TabPanel value={tabValue} index={4}>
          <Card sx={{ p: 3 }}>
            <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold' }}>
              Population Trend Over Decades
            </Typography>
            <Typography variant="body2" sx={{ mb: 3, color: '#6b7280' }}>
              Total registered births by decade.
            </Typography>
            {byDecadeLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : byDecadeError ? (
              <Alert severity="error">Failed to load decade data</Alert>
            ) : (
              <ResponsiveContainer width="100%" height={400}>
                <LineChart data={processedByDecadeData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis 
                    dataKey="decade" 
                    stroke="#666"
                  />
                  <YAxis stroke="#666" />
                  <Tooltip 
                    formatter={(value) => [value.toLocaleString(), 'Births']}
                  />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="Population"
                    name="Total Births"
                    stroke={COLORS.primary}
                    strokeWidth={3}
                    dot={{ fill: COLORS.primary, strokeWidth: 2, r: 4 }}
                    activeDot={{ r: 6 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            )}
          </Card>
        </TabPanel>

      </Box>
    </Stack>
  );
}
