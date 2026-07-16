import ScheduleIcon from "@mui/icons-material/Schedule";
import {
   Box,
   Card,
   CardContent,
   Chip,
   Stack,
   ToggleButton,
   ToggleButtonGroup,
   Tooltip,
   Typography,
} from "@mui/material";
import { MONO_FONT_FAMILY } from "../styles";
import { describeCron, formatNextRun } from "./cron";
import SetScheduleDialog from "./SetScheduleDialog";

type Scope = "package" | "version";

type ScheduleCardProps = {
   /** The package's cron, or null when no schedule is declared. */
   schedule: string | null;
   /** The package's persist scope. */
   scope: Scope;
   /** When set, the package is control-plane-driven (orchestrated). */
   manifestLocation: string | null;
   /** True when the package declares a freshness policy (schedule N/A). */
   hasFreshness: boolean;
   mutable: boolean;
   isSubmitting: boolean;
   /** True while a scope change is in flight. */
   isScopeMutating?: boolean;
   onSubmit: (schedule: string | null) => Promise<unknown>;
   /** Persist a new scope (package | version). */
   onScopeChange?: (scope: Scope) => Promise<unknown>;
};

export default function ScheduleCard({
   schedule,
   scope,
   manifestLocation,
   hasFreshness,
   mutable,
   isSubmitting,
   isScopeMutating,
   onSubmit,
   onScopeChange,
}: ScheduleCardProps) {
   const orchestrated = Boolean(manifestLocation);
   const info = schedule ? describeCron(schedule) : null;
   // A schedule requires version scope, so scope is locked while one is set —
   // the server would reject scope: package (publish-gate Rule 2). Clearing the
   // schedule unlocks it, giving an explicit way back to package scope.
   const scopeLocked = Boolean(schedule);

   return (
      <Card variant="outlined" sx={{ borderRadius: 2, mb: 6 }}>
         <CardContent>
            <Stack
               direction="row"
               justifyContent="space-between"
               alignItems="flex-start"
               spacing={2}
            >
               <Box sx={{ minWidth: 0 }}>
                  <Stack
                     direction="row"
                     alignItems="center"
                     spacing={1}
                     sx={{ mb: 1 }}
                  >
                     <ScheduleIcon fontSize="small" color="action" />
                     <Typography
                        variant="h6"
                        sx={{ fontWeight: 600, letterSpacing: "-0.025em" }}
                     >
                        Schedule
                     </Typography>
                  </Stack>

                  {orchestrated ? (
                     <Box>
                        <Chip
                           size="small"
                           color="default"
                           variant="outlined"
                           label="Control-plane managed"
                           sx={{ mb: 1 }}
                        />
                        <Typography variant="body2" color="text.secondary">
                           Refresh is driven by the control plane; the built-in
                           scheduler does not fire this package.
                        </Typography>
                     </Box>
                  ) : info ? (
                     <Box>
                        <Stack
                           direction="row"
                           alignItems="center"
                           spacing={1}
                           sx={{ mb: 0.5, flexWrap: "wrap" }}
                        >
                           <Chip
                              size="small"
                              color="success"
                              variant="outlined"
                              icon={<ScheduleIcon fontSize="small" />}
                              label="Active"
                           />
                           <Box
                              component="code"
                              sx={{
                                 fontFamily: MONO_FONT_FAMILY,
                                 fontSize: "0.8125rem",
                                 px: 0.75,
                                 py: 0.25,
                                 borderRadius: 1,
                                 bgcolor: "action.hover",
                              }}
                           >
                              {schedule}
                           </Box>
                        </Stack>
                        <Typography variant="body2">
                           {info.valid
                              ? info.description
                              : "Unrecognized cron expression."}
                        </Typography>
                        {info.valid && (
                           <Typography variant="body2" color="text.secondary">
                              Next run: {formatNextRun(info.nextRun)}
                           </Typography>
                        )}
                     </Box>
                  ) : hasFreshness ? (
                     <Typography variant="body2" color="text.secondary">
                        This package uses a freshness policy; a cron schedule is
                        not applicable.
                     </Typography>
                  ) : (
                     <Typography variant="body2" color="text.secondary">
                        On-demand only — no schedule set.
                     </Typography>
                  )}

                  {!orchestrated && (
                     <Box sx={{ mt: 2 }}>
                        <Typography
                           variant="caption"
                           color="text.secondary"
                           sx={{ display: "block", mb: 0.5 }}
                        >
                           Scope
                        </Typography>
                        <Tooltip
                           title={
                              scopeLocked
                                 ? "A schedule requires version scope. Clear the schedule to change it."
                                 : ""
                           }
                        >
                           <span>
                              <ToggleButtonGroup
                                 size="small"
                                 exclusive
                                 value={scope}
                                 disabled={
                                    !mutable ||
                                    !onScopeChange ||
                                    scopeLocked ||
                                    Boolean(isScopeMutating)
                                 }
                                 onChange={(_e, next: Scope | null) => {
                                    if (next && next !== scope) {
                                       onScopeChange?.(next);
                                    }
                                 }}
                              >
                                 <ToggleButton value="package">
                                    package
                                 </ToggleButton>
                                 <ToggleButton value="version">
                                    version
                                 </ToggleButton>
                              </ToggleButtonGroup>
                           </span>
                        </Tooltip>
                        <Typography
                           variant="caption"
                           color="text.secondary"
                           sx={{ display: "block", mt: 0.5 }}
                        >
                           {scope === "version"
                              ? "Materialized artifacts are per version (required for a schedule)."
                              : "Materialized artifacts are shared across versions."}
                        </Typography>
                     </Box>
                  )}

                  <Typography
                     variant="caption"
                     color="text.secondary"
                     sx={{ display: "block", mt: 1.5 }}
                  >
                     Declared in <code>publisher.json</code>.
                     {!orchestrated && (
                        <>
                           {" "}
                           In a hosted deployment, a change takes effect on your
                           next publish.
                        </>
                     )}
                  </Typography>
               </Box>

               {mutable && !orchestrated && (
                  <SetScheduleDialog
                     currentSchedule={schedule}
                     isSubmitting={isSubmitting}
                     disabled={hasFreshness}
                     disabledReason="This package declares a freshness policy; a schedule and freshness are mutually exclusive."
                     onSubmit={onSubmit}
                  />
               )}
            </Stack>
         </CardContent>
      </Card>
   );
}
