function shouldLog(level: LogLevel): boolean {
   return (
      LEVEL_PRIORITY[level] !== undefined &&
      LEVEL_PRIORITY[CURRENT_LEVEL] !== undefined &&
      LEVEL_PRIORITY[level] <= LEVEL_PRIORITY[CURRENT_LEVEL]
   );
}

const LOG_LEVELS = ["error", "warn", "info", "debug"] as const;

type LogLevel = (typeof LOG_LEVELS)[number];

const LEVEL_PRIORITY: Record<LogLevel, number> = {
   error: 0,
   warn: 1,
   info: 2,
   debug: 3,
};

let CURRENT_LEVEL: LogLevel = "debug";

function isLogLevel(value: string): value is LogLevel {
   return LOG_LEVELS.includes(value as LogLevel);
}

export const setLogLevel = (level: string) => {
   if (isLogLevel(level)) {
      CURRENT_LEVEL = level;
   }
};
export function error(msg: string) {
   if (shouldLog("error")) {
      console.error(`[k6][ERROR] ${msg}`);
   }
}

export function warn(msg: string) {
   if (shouldLog("warn")) {
      console.warn(`[k6][WARN] ${msg}`);
   }
}

export function info(msg: string) {
   if (shouldLog("info")) {
      console.info(`[k6][INFO] ${msg}`);
   }
}

export function debug(msg: string) {
   if (shouldLog("debug")) {
      console.debug(`[k6][DEBUG] ${msg}`);
   }
}

// Export logger object with methods
export const logger = {
   error,
   warn,
   info,
   debug,
   setLogLevel,
};
