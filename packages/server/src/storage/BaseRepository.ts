import { DatabaseConnection } from "./DatabaseInterface";

export abstract class BaseRepository {
   constructor(protected db: DatabaseConnection) {}

   protected generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
   }

   protected now(): Date {
      return new Date();
   }

   protected async executeQuery<T>(
      _query: string,
      _params?: unknown[],
   ): Promise<T[]> {
      // This will be implemented by specific database implementations
      throw new Error(
         "executeQuery must be implemented by database-specific class",
      );
   }

   protected async executeOne<T>(
      query: string,
      params?: unknown[],
   ): Promise<T | null> {
      const results = await this.executeQuery<T>(query, params);
      return results.length > 0 ? results[0] : null;
   }
}
