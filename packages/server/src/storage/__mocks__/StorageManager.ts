export class StorageManager {
   async initialize() {
      return;
   }
   getRepository() {
      return {
         getProjects: async () => [],
         createProject: async (data: any) => ({ id: "test-id", ...data }),
         updateProject: async (id: string, data: any) => ({ id, ...data }),
         getPackages: async () => [],
         createPackage: async (data: any) => ({ id: "test-id", ...data }),
         updatePackage: async (id: string, data: any) => ({ id, ...data }),
         deletePackage: async () => {},
         getConnections: async () => [],
         createConnection: async (data: any) => ({ id: "test-id", ...data }),
         updateConnection: async (id: string, data: any) => ({ id, ...data }),
         deleteConnection: async () => {},
      };
   }
}
