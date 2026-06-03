import { AxiosError } from "axios";
import { logAxiosError } from "../utils/logger.js";
import {
  Configuration,
  ConnectionsApi,
  CreateMaterializationRequest,
  DatabasesApi,
  EnvironmentsApi,
  ManifestActionActionEnum,
  ManifestsApi,
  MaterializationActionActionEnum,
  MaterializationsApi,
  ModelsApi,
  NotebooksApi,
  PackagesApi,
} from "./generated";

export class PublisherClient {
  private environmentsApi: EnvironmentsApi;
  private packagesApi: PackagesApi;
  private connectionsApi: ConnectionsApi;
  private materializationsApi: MaterializationsApi;
  private manifestsApi: ManifestsApi;
  private modelsApi: ModelsApi;
  private notebooksApi: NotebooksApi;
  private databasesApi: DatabasesApi;
  private baseURL: string;

  constructor(urlOverride?: string) {
    this.baseURL = this.resolveServerURL(urlOverride);

    const config = new Configuration({
      basePath: `${this.baseURL}/api/v0`,
    });

    this.environmentsApi = new EnvironmentsApi(config);
    this.packagesApi = new PackagesApi(config);
    this.connectionsApi = new ConnectionsApi(config);
    this.materializationsApi = new MaterializationsApi(config);
    this.manifestsApi = new ManifestsApi(config);
    this.modelsApi = new ModelsApi(config);
    this.notebooksApi = new NotebooksApi(config);
    this.databasesApi = new DatabasesApi(config);
  }

  private resolveServerURL(urlOverride?: string): string {
    return (
      urlOverride || process.env.MALLOY_PUBLISHER_URL || "http://localhost:4000"
    );
  }

  getBaseURL(): string {
    return this.baseURL;
  }

  // Environments
  async listEnvironments(): Promise<any[]> {
    try {
      const response = await this.environmentsApi.listEnvironments();
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async getEnvironment(name: string): Promise<any> {
    try {
      const response = await this.environmentsApi.getEnvironment(name);
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async createEnvironment(name: string): Promise<any> {
    try {
      const response = await this.environmentsApi.createEnvironment({ name });
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async updateEnvironment(
    name: string,
    updates: { name?: string; readme?: string; location?: string },
  ): Promise<any> {
    try {
      const response = await this.environmentsApi.updateEnvironment(
        name,
        updates,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async deleteEnvironment(name: string): Promise<void> {
    try {
      await this.environmentsApi.deleteEnvironment(name);
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  // Packages
  async listPackages(environmentName: string): Promise<any[]> {
    try {
      const response = await this.packagesApi.listPackages(environmentName);
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async getPackage(environmentName: string, packageName: string): Promise<any> {
    try {
      const response = await this.packagesApi.getPackage(
        environmentName,
        packageName,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async createPackage(
    environmentName: string,
    packageName: string,
    location: string,
    description?: string,
  ): Promise<any> {
    try {
      const response = await this.packagesApi.createPackage(environmentName, {
        name: packageName,
        location,
        description,
      });
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async updatePackage(
    environmentName: string,
    packageName: string,
    updates: { name?: string; location?: string; description?: string },
  ): Promise<any> {
    try {
      const response = await this.packagesApi.updatePackage(
        environmentName,
        packageName,
        updates,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async deletePackage(
    environmentName: string,
    packageName: string,
  ): Promise<void> {
    try {
      await this.packagesApi.deletePackage(environmentName, packageName);
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  // Connections
  async listConnections(environmentName: string): Promise<any[]> {
    try {
      const response =
        await this.connectionsApi.listConnections(environmentName);
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async getConnection(
    environmentName: string,
    connectionName: string,
  ): Promise<any> {
    try {
      const response = await this.connectionsApi.getConnection(
        environmentName,
        connectionName,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async createConnection(
    environmentName: string,
    connection: any,
  ): Promise<any> {
    try {
      // Extract connection name from the connection object
      const connectionName = connection.name;
      if (!connectionName) {
        throw new Error('Connection object must have a "name" property');
      }

      const response = await this.connectionsApi.createConnection(
        environmentName,
        connectionName,
        connection,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async updateConnection(
    environmentName: string,
    connectionName: string,
    connection: any,
  ): Promise<any> {
    try {
      const response = await this.connectionsApi.updateConnection(
        environmentName,
        connectionName,
        connection,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async deleteConnection(
    environmentName: string,
    connectionName: string,
  ): Promise<void> {
    try {
      await this.connectionsApi.deleteConnection(
        environmentName,
        connectionName,
      );
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  // Materializations
  async listMaterializations(
    environmentName: string,
    packageName: string,
    limit?: number,
    offset?: number,
  ): Promise<any[]> {
    try {
      const response = await this.materializationsApi.listMaterializations(
        environmentName,
        packageName,
        limit,
        offset,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async getMaterialization(
    environmentName: string,
    packageName: string,
    materializationId: string,
  ): Promise<any> {
    try {
      const response = await this.materializationsApi.getMaterialization(
        environmentName,
        packageName,
        materializationId,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async createMaterialization(
    environmentName: string,
    packageName: string,
    request: CreateMaterializationRequest,
  ): Promise<any> {
    try {
      const response = await this.materializationsApi.createMaterialization(
        environmentName,
        packageName,
        request,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async materializationAction(
    environmentName: string,
    packageName: string,
    materializationId: string,
    action: "start" | "stop",
  ): Promise<any> {
    try {
      const response = await this.materializationsApi.materializationAction(
        environmentName,
        packageName,
        materializationId,
        action as MaterializationActionActionEnum,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async deleteMaterialization(
    environmentName: string,
    packageName: string,
    materializationId: string,
  ): Promise<void> {
    try {
      await this.materializationsApi.deleteMaterialization(
        environmentName,
        packageName,
        materializationId,
      );
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  // Build manifest
  async getManifest(
    environmentName: string,
    packageName: string,
  ): Promise<any> {
    try {
      const response = await this.manifestsApi.getManifest(
        environmentName,
        packageName,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async reloadManifest(
    environmentName: string,
    packageName: string,
  ): Promise<any> {
    try {
      const response = await this.manifestsApi.manifestAction(
        environmentName,
        packageName,
        ManifestActionActionEnum.Reload,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  // Models (read-only)
  async listModels(
    environmentName: string,
    packageName: string,
  ): Promise<any[]> {
    try {
      const response = await this.modelsApi.listModels(
        environmentName,
        packageName,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async getModel(
    environmentName: string,
    packageName: string,
    path: string,
  ): Promise<any> {
    try {
      const response = await this.modelsApi.getModel(
        environmentName,
        packageName,
        path,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  // Notebooks (read-only)
  async listNotebooks(
    environmentName: string,
    packageName: string,
  ): Promise<any[]> {
    try {
      const response = await this.notebooksApi.listNotebooks(
        environmentName,
        packageName,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async getNotebook(
    environmentName: string,
    packageName: string,
    path: string,
  ): Promise<any> {
    try {
      const response = await this.notebooksApi.getNotebook(
        environmentName,
        packageName,
        path,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  // Databases (read-only)
  async listDatabases(
    environmentName: string,
    packageName: string,
  ): Promise<any[]> {
    try {
      const response = await this.databasesApi.listDatabases(
        environmentName,
        packageName,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  private handleError(error: AxiosError): Error {
    logAxiosError(error);
    if (error.response) {
      const message =
        (error.response.data as any)?.message || error.response.statusText;
      return new Error(
        `Publisher API Error (${error.response.status}): ${message}`,
      );
    } else if (error.request) {
      return new Error(
        `Cannot reach Publisher at ${this.baseURL}. Is the server running?`,
      );
    } else {
      return new Error(`Request error: ${error.message}`);
    }
  }
}
