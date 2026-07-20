import { useEffect, useState } from "react";
import {
  Box,
  Stack,
  Typography,
  Tooltip,
  Fab,
  IconButton,
} from "@mui/material";
import GridLayout from "react-grid-layout";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

import AddIcon from "@mui/icons-material/Add";
import LockIcon from "@mui/icons-material/Lock";
import LockOpenIcon from "@mui/icons-material/LockOpen";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";

import Header from "./Header";
import { Widget } from "../types/widget";
import { getNextWidgetPosition } from "../utils/getNextWidgetPosition";
import { v4 as uuidv4 } from "uuid";
import { EmbeddedQueryResult } from "@malloy-publisher/sdk";
import AddChartDialog from "./AddChartDialog";

export default function Dashboard({
  selectedView,
  storageKey,
  defaultWidgets = [],
  customizeWidgetsEffect,
  resourceUri,
}: {
  selectedView:
    | "storefront"
    | "singleEmbed"
    | "dynamicDashboard"
    | "interactive";
  storageKey: string;
  defaultWidgets?: Widget[];
  customizeWidgetsEffect?: (widgets: Widget[]) => void;
  resourceUri: string;
}) {
  const [widgets, setWidgets] = useState<Widget[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  const handleAddWidget = (newTitle: string, newQuery: string) => {
    const id = uuidv4();
    const widgetWidth = 12;
    const widgetHeight = 10;
    const cols = 12;

    const { x, y } = getNextWidgetPosition(widgets, widgetWidth, cols);

    const newWidget: Widget = {
      id,
      queryResultString: newQuery,
      title: newTitle.trim() !== "" ? newTitle.trim() : undefined,
      layout: {
        i: id,
        x,
        y,
        w: widgetWidth,
        h: widgetHeight,
        static: false,
      },
      locked: false,
    };

    setWidgets((prev) => [...prev, newWidget]);
    setIsDialogOpen(false);
  };

  const handleRemove = (id: string) => {
    setWidgets((prev) => prev.filter((w) => w.id !== id));
  };

  const onLayoutChange = (newLayout: any[]) => {
    setWidgets((prev) =>
      prev.map((widget) => {
        const layoutItem = newLayout.find((l) => l.i === widget.id);
        return layoutItem ? { ...widget, layout: layoutItem } : widget;
      })
    );
  };

  const handleToggleLock = (id: string) => {
    setWidgets((prev) =>
      prev.map((widget) =>
        widget.id === id
          ? {
              ...widget,
              locked: !widget.locked,
              layout: { ...widget.layout, static: !widget.locked },
            }
          : widget
      )
    );
  };

  useEffect(() => {
    if (loaded) {
      localStorage.setItem(storageKey, JSON.stringify(widgets));
    }
  }, [widgets, loaded, storageKey]);

  useEffect(() => {
    const saved = localStorage.getItem(storageKey);
    if (saved) {
      try {
        const parsed: Widget[] = JSON.parse(saved);
        setWidgets(parsed);
      } catch (err) {
        console.error("Failed to parse saved dashboard", err);
      }
    } else {
      setWidgets(defaultWidgets);
    }
    setLoaded(true);
  }, [defaultWidgets, storageKey]);

  // optional "customize" hook for StorefrontDashboard
  useEffect(() => {
    if (customizeWidgetsEffect) {
      customizeWidgetsEffect(widgets);
    }
  }, [customizeWidgetsEffect, widgets]);

  return (
    <Stack spacing={2} sx={{ mt: { xs: 8, md: 0 }, mb: 8 }}>
      <Header selectedView={selectedView} />

      {widgets.length === 0 ? (
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            alignItems: "center",
            justifyContent: "center",
            minHeight: "400px",
            textAlign: "center",
          }}
        >
          <Typography variant="h6" sx={{ mb: 2, color: "text.secondary" }}>
            Click{" "}
            <Tooltip title="Add embedded chart" arrow>
              <IconButton
                color="primary"
                onClick={() => setIsDialogOpen(true)}
                sx={{
                  mx: 1,
                  backgroundColor: "primary.main",
                  color: "white",
                  "&:hover": {
                    backgroundColor: "primary.dark",
                  },
                }}
              >
                <AddIcon />
              </IconButton>
            </Tooltip>{" "}
            to add a new Chart
          </Typography>
        </Box>
      ) : (
        <GridLayout
          className="layout"
          layout={widgets.map((w) => w.layout)}
          cols={12}
          rowHeight={40}
          width={1200}
          onLayoutChange={onLayoutChange}
          draggableHandle=".drag-handle"
        >
          {widgets.map((widget) => (
            <div
              key={widget.id}
              data-grid={{ ...widget.layout, i: widget.id }}
              style={{
                border: "1px solid #ccc",
                padding: 8,
                borderRadius: 4,
                overflow: "hidden",
                maxWidth: "100%",
                boxSizing: "border-box",
              }}
            >
              <Box
                sx={{
                  mb: 1,
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                }}
              >
                <Typography
                  variant="subtitle2"
                  className="drag-handle"
                  sx={{ cursor: "move" }}
                >
                  {widget.title}
                </Typography>

                <Box sx={{ display: "flex", gap: 1 }}>
                  <Tooltip
                    title={widget.locked ? "Unlock chart" : "Lock chart"}
                    arrow
                  >
                    <IconButton
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        e.preventDefault();
                        handleToggleLock(widget.id);
                      }}
                    >
                      {widget.locked ? <LockIcon /> : <LockOpenIcon />}
                    </IconButton>
                  </Tooltip>

                  <Tooltip title="Remove chart" arrow>
                    <IconButton
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        e.preventDefault();
                        handleRemove(widget.id);
                      }}
                    >
                      <DeleteOutlineIcon />
                    </IconButton>
                  </Tooltip>
                </Box>
              </Box>

              <Box
                sx={{
                  width: "90%",
                  height: "396px",
                  overflow: "visible",
                }}
              >
                <EmbeddedQueryResult
                  embeddedQueryResult={widget.queryResultString}
                />
              </Box>
            </div>
          ))}
        </GridLayout>
      )}

      <Tooltip title="Add embedded chart" arrow>
        <Fab
          color="primary"
          aria-label="add"
          onClick={() => setIsDialogOpen(true)}
          disabled={isDialogOpen}
          sx={{
            position: "fixed",
            bottom: 24,
            right: 24,
            zIndex: isDialogOpen ? 1 : 200,
          }}
        >
          <AddIcon />
        </Fab>
      </Tooltip>
      {isDialogOpen && (
        <AddChartDialog
          onClose={() => setIsDialogOpen(false)}
          handleAddWidget={handleAddWidget}
          resourceUri={resourceUri}
        />
      )}
    </Stack>
  );
}
