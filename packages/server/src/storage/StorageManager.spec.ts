type MockData = Record<string, unknown>;

export class StorageManager {
   async initialize(): Promise<void> {
      return;
   }

   getRepository() {
      return {
         listProjects: async (): Promise<unknown[]> => [],
         createProject: async (data: MockData): Promise<MockData> => ({
            id: "test-id",
            ...data,
         }),
         updateProject: async (
            id: string,
            data: MockData,
         ): Promise<MockData> => ({
            id,
            ...data,
         }),
         getPackages: async (): Promise<unknown[]> => [],
         createPackage: async (data: MockData): Promise<MockData> => ({
            id: "test-id",
            ...data,
         }),
         updatePackage: async (
            id: string,
            data: MockData,
         ): Promise<MockData> => ({
            id,
            ...data,
         }),
         deletePackage: async (): Promise<void> => {},
         getConnections: async (): Promise<unknown[]> => [],
         createConnection: async (data: MockData): Promise<MockData> => ({
            id: "test-id",
            ...data,
         }),
         updateConnection: async (
            id: string,
            data: MockData,
         ): Promise<MockData> => ({
            id,
            ...data,
         }),
         deleteConnection: async (): Promise<void> => {},
      };
   }
}
