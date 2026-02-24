import winston from "winston";
import { AxiosError } from "axios";

// Determine log level from environment variable
// Valid levels: error, warn, info, verbose, debug, silly
// Default: 'info' (less verbose than server for CLI)
const VALID_LOG_LEVELS = ["error", "warn", "info", "verbose", "debug", "silly"];

const getLogLevel = (): string => {
  if (process.env.LOG_LEVEL) {
    const logLevel = process.env.LOG_LEVEL.toLowerCase();
    if (VALID_LOG_LEVELS.includes(logLevel)) {
      return logLevel;
    } else {
      console.error(
        `Invalid log level: ${process.env.LOG_LEVEL}. Valid log levels are: ${VALID_LOG_LEVELS.join(", ")}. Defaulting to "info".`,
      );
    }
  }
  return "info";
};

// CLI logger - always use colorized console output
export const logger = winston.createLogger({
  level: getLogLevel(),
  format: winston.format.combine(
    winston.format.colorize(),
    winston.format.timestamp({ format: "HH:mm:ss" }),
    winston.format.printf(({ level, message, timestamp, ...metadata }) => {
      let msg = `${timestamp} [${level}]: ${message}`;

      // Include metadata if present (excluding timestamp)
      const metadataKeys = Object.keys(metadata);
      if (metadataKeys.length > 0) {
        msg += ` ${JSON.stringify(metadata)}`;
      }

      return msg;
    }),
  ),
  transports: [new winston.transports.Console()],
});

const rawLogger = winston.createLogger({
  level: "info",
  format: winston.format.printf((info: any) => info.message),
  transports: [new winston.transports.Console()],
});

/**
 * Format duration in milliseconds to a human-readable string with unit
 * @param durationMs Duration in milliseconds
 * @returns Formatted string with 2 decimal places and unit (s or ms)
 */
export function formatDuration(durationMs: number): string {
  // If duration is >= 1000ms, show in seconds, otherwise show in milliseconds
  if (durationMs >= 1000) {
    const seconds = durationMs / 1000;
    return `${seconds.toFixed(2)}s`;
  }
  return `${durationMs.toFixed(2)}ms`;
}

/**
 * Log axios errors with detailed information
 */
export const logAxiosError = (error: AxiosError) => {
  if (error.response) {
    // The request was made and the server responded with a status code
    // that falls out of the range of 2xx
    logger.error("Publisher API error", {
      url: error.response.config.url,
      status: error.response.status,
      data: error.response.data,
    });
  } else if (error.request) {
    // The request was made but no response was received
    logger.error("Network error - no response received", {
      url: error.config?.url,
    });
  } else {
    // Something happened in setting up the request that triggered an Error
    logger.error("Request setup error", { message: error.message });
  }
};

// Convenience methods for CLI commands
export const logSuccess = (
  message: string,
  metadata?: Record<string, unknown>,
) => {
  logger.info(`✓ ${message}`, metadata);
};

export const logError = (message: string, error?: Error | AxiosError) => {
  if (error && "isAxiosError" in error) {
    logAxiosError(error as AxiosError);
  } else {
    logger.error(
      message,
      error ? { error: error.message, stack: error.stack } : undefined,
    );
  }
};

export const logWarning = (
  message: string,
  metadata?: Record<string, unknown>,
) => {
  logger.warn(message, metadata);
};

export const logInfo = (
  message: string,
  metadata?: Record<string, unknown>,
) => {
  logger.info(message, metadata);
};

export const logDebug = (
  message: string,
  metadata?: Record<string, unknown>,
) => {
  logger.debug(message, metadata);
};

export const logOutput = (content: string) => {
  rawLogger.info(content);
};
