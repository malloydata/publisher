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

const usd = (value: number) =>
  value.toLocaleString(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });

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
        // Using the storefront package, which is DuckDB based and needs no cloud credentials
        const response = await fetch(`/api/v0/environments/examples/packages/storefront/models/${modelPath}/query`, {
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

// Order fulfillment statuses present in the storefront sample, in chart-stacking order.
const STATUSES = ['Complete', 'Shipped', 'Processing', 'Returned', 'Cancelled'];
const STATUS_COLORS: { [key: string]: string } = {
  Complete: COLORS.secondary,
  Shipped: COLORS.primary,
  Processing: COLORS.accent,
  Returned: COLORS.pink,
  Cancelled: COLORS.purple,
};

export default function InteractiveDashboard({
  selectedView,
}: {
  selectedView: "storefront" | "singleEmbed" | "dynamicDashboard" | "interactive";
}) {
  const [tabValue, setTabValue] = useState(0);
  const [stateFilter, setStateFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
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

  const handleStatusFilterChange = React.useCallback((event: SelectChangeEvent) => {
    event.preventDefault();

    // Store current scroll position
    const currentScroll = window.pageYOffset;
    scrollPositionRef.current = currentScroll;

    setStatusFilter(event.target.value);

    // Prevent scroll to top by restoring position immediately
    requestAnimationFrame(() => {
      window.scrollTo(0, currentScroll);
    });
  }, []);

  // Build filtered query with where clause appended
  const buildFilteredQuery = (baseQuery: string) => {
    const conditions = [];
    if (stateFilter) {
      conditions.push(`customers.state = '${stateFilter}'`);
    }
    if (statusFilter) {
      conditions.push(`status = '${statusFilter}'`);
    }

    if (conditions.length > 0) {
      return `${baseQuery} + { where: ${conditions.join(' and ')} }`;
    }
    return baseQuery;
  };

  // Query against order_items, the fact source in storefront.malloy
  const MODEL_PATH = 'storefront.malloy';

  // Queries for the dashboard
  const topProductsQuery = buildFilteredQuery(`run: order_items -> {
    group_by: products.name
    aggregate: total_sales
    limit: 10
  }`);

  const byStateQuery = buildFilteredQuery(`run: order_items -> {
    group_by: customers.state
    aggregate: total_sales
    limit: 10
  }`);

  const byStatusQuery = buildFilteredQuery(`run: order_items -> {
    group_by: status
    aggregate: total_sales
  }`);

  const byYearQuery = buildFilteredQuery(`run: order_items -> {
    group_by: created_year is year(created_at)
    aggregate: total_sales
    order_by: created_year
  }`);

  const byStateStatusQuery = buildFilteredQuery(`run: order_items -> {
    group_by: customers.state, status
    aggregate: total_sales
    limit: 60
  }`);

  // Use the custom hook to fetch raw data
  const { data: topProductsData, isLoading: topProductsLoading, isError: topProductsError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: topProductsQuery,
  });

  const { data: byStateData, isLoading: byStateLoading, isError: byStateError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: byStateQuery,
  });

  const { data: byStatusData, isLoading: byStatusLoading, isError: byStatusError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: byStatusQuery,
  });

  const { data: byYearData, isLoading: byYearLoading, isError: byYearError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: byYearQuery,
  });

  const { data: byStateStatusData, isLoading: byStateStatusLoading, isError: byStateStatusError } = useRawQueryData({
    modelPath: MODEL_PATH,
    query: byStateStatusQuery,
  });

  // Process data for charts
  const processedTopProductsData = topProductsData?.map((item: any, index: number) => ({
    name: item.name || 'Unknown',
    sales: Number(item.total_sales) || 0,
    fill: COLORS.chartColors[index % COLORS.chartColors.length]
  })) || [];

  const processedByStateData = byStateData?.map((item: any, index: number) => ({
    name: item.state || 'Unknown',
    value: Number(item.total_sales) || 0,
    fill: COLORS.chartColors[index % COLORS.chartColors.length]
  })) || [];

  const processedByStatusData = byStatusData?.map((item: any) => ({
    name: item.status || 'Unknown',
    sales: Number(item.total_sales) || 0,
    fill: STATUS_COLORS[item.status] || COLORS.primary
  })) || [];

  const processedByYearData = byYearData?.map((item: any) => ({
    year: item.created_year?.toString() || 'Unknown',
    'Sales': Number(item.total_sales) || 0
  })) || [];

  // Process stacked bar data: sales by state, stacked by fulfillment status
  const processedStackedData = React.useMemo(() => {
    if (!byStateStatusData || byStateStatusData.length === 0) return [];

    const stateMap: {[key: string]: any} = {};
    byStateStatusData.forEach((item: any) => {
      const state = item.state;
      if (!stateMap[state]) {
        stateMap[state] = { name: state };
        STATUSES.forEach((s) => { stateMap[state][s] = 0; });
      }
      if (item.status) {
        stateMap[state][item.status] = Number(item.total_sales);
      }
    });

    const total = (row: any) => STATUSES.reduce((sum, s) => sum + (row[s] || 0), 0);
    return Object.values(stateMap).sort((a: any, b: any) => total(b) - total(a)).slice(0, 10);
  }, [byStateStatusData]);

  const tabs = [
    { id: 0, label: 'Top Products' },
    { id: 1, label: 'By State' },
    { id: 2, label: 'By Status' },
    { id: 3, label: 'State & Status' },
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
          Storefront Sales Explorer
        </Typography>
        <Typography variant="body1" sx={{ mb: 4, color: '#6b7280' }}>
          Explore the storefront sample's order line items with interactive filtering.
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
            <Typography variant="h4" sx={{ fontWeight: 'bold' }}>$2.1M</Typography>
            <Typography variant="body2">Total Sales</Typography>
          </Card>
          <Card sx={{
            p: 3,
            background: 'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)',
            color: 'white',
            boxShadow: '0 8px 32px rgba(240, 147, 251, 0.3)',
            flex: 1
          }}>
            <Typography variant="h4" sx={{ fontWeight: 'bold' }}>11K</Typography>
            <Typography variant="body2">Orders</Typography>
          </Card>
          <Card sx={{
            p: 3,
            background: 'linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)',
            color: 'white',
            boxShadow: '0 8px 32px rgba(79, 172, 254, 0.3)',
            flex: 1
          }}>
            <Typography variant="h4" sx={{ fontWeight: 'bold' }}>2023–2025</Typography>
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
                <MenuItem value="California">California</MenuItem>
                <MenuItem value="Texas">Texas</MenuItem>
                <MenuItem value="New York">New York</MenuItem>
                <MenuItem value="Illinois">Illinois</MenuItem>
                <MenuItem value="Florida">Florida</MenuItem>
                <MenuItem value="Washington">Washington</MenuItem>
                <MenuItem value="Massachusetts">Massachusetts</MenuItem>
                <MenuItem value="Georgia">Georgia</MenuItem>
                <MenuItem value="Colorado">Colorado</MenuItem>
                <MenuItem value="Oregon">Oregon</MenuItem>
              </Select>
            </FormControl>

            <FormControl sx={{ minWidth: 200 }}>
              <InputLabel>Filter by Status</InputLabel>
              <Select
                value={statusFilter}
                label="Filter by Status"
                onChange={handleStatusFilterChange}
              >
                <MenuItem value="">All Statuses</MenuItem>
                <MenuItem value="Complete">Complete</MenuItem>
                <MenuItem value="Shipped">Shipped</MenuItem>
                <MenuItem value="Processing">Processing</MenuItem>
                <MenuItem value="Returned">Returned</MenuItem>
                <MenuItem value="Cancelled">Cancelled</MenuItem>
              </Select>
            </FormControl>
          </Stack>

          {(stateFilter || statusFilter) && (
            <Alert severity="info" sx={{ mt: 2 }}>
              <Typography variant="body2">
                <strong>Active Filters:</strong>{' '}
                {stateFilter && `State = "${stateFilter}"`}
                {stateFilter && statusFilter && ' AND '}
                {statusFilter && `Status = "${statusFilter}"`}
              </Typography>
              <Typography variant="body2" sx={{ mt: 1, fontFamily: 'monospace', fontSize: '0.8rem' }}>
                Query modification: + where: {[
                  stateFilter && `customers.state = '${stateFilter}'`,
                  statusFilter && `status = '${statusFilter}'`
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
              Top 10 Products by Sales
            </Typography>
            {topProductsLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : topProductsError ? (
              <Alert severity="error">Failed to load product data</Alert>
            ) : (
              <ResponsiveContainer width="100%" height={350}>
                <BarChart
                  data={processedTopProductsData}
                  margin={{ top: 20, right: 30, left: 20, bottom: 50 }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis
                    dataKey="name"
                    stroke="#666"
                  />
                  <YAxis stroke="#666" />
                  <Tooltip
                    formatter={(value) => [usd(Number(value)), 'Sales']}
                  />
                  <Bar dataKey="sales" fill="#3B82F6" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </Card>
        </TabPanel>

        <TabPanel value={tabValue} index={1}>
          <Card sx={{ p: 3 }}>
            <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold' }}>
              Sales by State (Top 10)
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
                    formatter={(value) => [usd(Number(value)), 'Sales']}
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
              Sales by Fulfillment Status
            </Typography>
            {byStatusLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : byStatusError ? (
              <Alert severity="error">Failed to load status data</Alert>
            ) : (
              <ResponsiveContainer width="100%" height={400}>
                <PieChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                  <Pie
                    data={processedByStatusData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={140}
                    dataKey="sales"
                    label={({ name }) => name}
                  >
                    {processedByStatusData.map((entry: any, index: number) => (
                      <Cell key={`cell-${index}`} fill={entry.fill} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value) => [usd(Number(value)), 'Sales']} />
                </PieChart>
              </ResponsiveContainer>
            )}
          </Card>
        </TabPanel>

        <TabPanel value={tabValue} index={3}>
          <Card sx={{ p: 3 }}>
            <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold' }}>
              Sales by State and Status
            </Typography>
            {byStateStatusLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : byStateStatusError ? (
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
                  <Tooltip formatter={(value) => [usd(Number(value)), 'Sales']} />
                  <Legend />
                  {STATUSES.map((status) => (
                    <Bar key={status} dataKey={status} stackId="a" fill={STATUS_COLORS[status]} />
                  ))}
                </BarChart>
              </ResponsiveContainer>
            )}
          </Card>
        </TabPanel>

        <TabPanel value={tabValue} index={4}>
          <Card sx={{ p: 3 }}>
            <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold' }}>
              Sales Trend Over Time
            </Typography>
            <Typography variant="body2" sx={{ mb: 3, color: '#6b7280' }}>
              Total sales by calendar year.
            </Typography>
            {byYearLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
                <CircularProgress />
              </Box>
            ) : byYearError ? (
              <Alert severity="error">Failed to load yearly data</Alert>
            ) : (
              <ResponsiveContainer width="100%" height={400}>
                <LineChart data={processedByYearData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                  <XAxis
                    dataKey="year"
                    stroke="#666"
                  />
                  <YAxis stroke="#666" />
                  <Tooltip
                    formatter={(value) => [usd(Number(value)), 'Sales']}
                  />
                  <Legend />
                  <Line
                    type="monotone"
                    dataKey="Sales"
                    name="Total Sales"
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
