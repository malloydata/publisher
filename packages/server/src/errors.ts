import { MalloyError } from "@malloydata/malloy";
import { PUBLISHER_CONFIG_NAME } from "./constants";

export function internalErrorToHttpError(error: Error) {
   if (error instanceof BadRequestError) {
      return httpError(400, error.message);
   } else if (error instanceof FrozenConfigError) {
      return httpError(403, error.message);
   } else if (error instanceof ProjectNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof PackageNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof ModelNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof MalloyError) {
      return httpError(400, error.message);
   } else if (error instanceof ConnectionNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof ModelCompilationError) {
      return httpError(424, error.message);
   } else if (error instanceof ConnectionError) {
      return httpError(502, error.message);
   } else if (error instanceof TaskNotFoundError) {
      return httpError(404, error.message);
   } else if (error instanceof TaskExecutionConflictError) {
      return httpError(409, error.message);
   } else if (error instanceof InvalidStateTransitionError) {
      return httpError(409, error.message);
   } else {
      return httpError(500, error.message);
   }
}

function httpError(code: number, message: string) {
   return {
      status: code,
      json: {
         code,
         message: message,
      },
   };
}

export class NotImplementedError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class BadRequestError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ProjectNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class PackageNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ModelNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ConnectionNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ConnectionError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class ModelCompilationError extends Error {
   constructor(error: MalloyError) {
      super(error.message);
   }
}

export class FrozenConfigError extends Error {
   constructor(
      message = `Publisher config can't be updated when ${PUBLISHER_CONFIG_NAME} has { "frozenConfig": true }`,
   ) {
      super(message);
   }
}

export class TaskNotFoundError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class TaskExecutionConflictError extends Error {
   constructor(message: string) {
      super(message);
   }
}

export class InvalidStateTransitionError extends Error {
   constructor(message: string) {
      super(message);
   }
}
