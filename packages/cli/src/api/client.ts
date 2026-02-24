import {
  Configuration,
  ProjectsApi,
  PackagesApi,
  ConnectionsApi,
} from "./generated";
import { AxiosError } from "axios";
import { logAxiosError } from "../utils/logger.js";

export class PublisherClient {
  private projectsApi: ProjectsApi;
  private packagesApi: PackagesApi;
  private connectionsApi: ConnectionsApi;
  private baseURL: string;

  constructor(urlOverride?: string) {
    this.baseURL = this.resolveServerURL(urlOverride);

    const config = new Configuration({
      basePath: `${this.baseURL}/api/v0`,
    });

    this.projectsApi = new ProjectsApi(config);
    this.packagesApi = new PackagesApi(config);
    this.connectionsApi = new ConnectionsApi(config);
  }

  private resolveServerURL(urlOverride?: string): string {
    return (
      urlOverride || process.env.MALLOY_PUBLISHER_URL || "http://localhost:4000"
    );
  }

  getBaseURL(): string {
    return this.baseURL;
  }

  // Projects
  async listProjects(): Promise<any[]> {
    try {
      const response = await this.projectsApi.listProjects();
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async getProject(name: string): Promise<any> {
    try {
      const response = await this.projectsApi.getProject(name);
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async createProject(name: string): Promise<any> {
    try {
      const response = await this.projectsApi.createProject({ name });
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async updateProject(
    name: string,
    updates: { name?: string; readme?: string; location?: string },
  ): Promise<any> {
    try {
      const response = await this.projectsApi.updateProject(name, updates);
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async deleteProject(name: string): Promise<void> {
    try {
      await this.projectsApi.deleteProject(name);
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  // Packages
  async listPackages(projectName: string): Promise<any[]> {
    try {
      const response = await this.packagesApi.listPackages(projectName);
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async getPackage(projectName: string, packageName: string): Promise<any> {
    try {
      const response = await this.packagesApi.getPackage(
        projectName,
        packageName,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async createPackage(
    projectName: string,
    packageName: string,
    location: string,
    description?: string,
  ): Promise<any> {
    try {
      const response = await this.packagesApi.createPackage(projectName, {
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
    projectName: string,
    packageName: string,
    updates: { name?: string; location?: string; description?: string },
  ): Promise<any> {
    try {
      const response = await this.packagesApi.updatePackage(
        projectName,
        packageName,
        updates,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async deletePackage(projectName: string, packageName: string): Promise<void> {
    try {
      await this.packagesApi.deletePackage(projectName, packageName);
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  // Connections
  async listConnections(projectName: string): Promise<any[]> {
    try {
      const response = await this.connectionsApi.listConnections(projectName);
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async getConnection(
    projectName: string,
    connectionName: string,
  ): Promise<any> {
    try {
      const response = await this.connectionsApi.getConnection(
        projectName,
        connectionName,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async createConnection(projectName: string, connection: any): Promise<any> {
    try {
      // Extract connection name from the connection object
      const connectionName = connection.name;
      if (!connectionName) {
        throw new Error('Connection object must have a "name" property');
      }

      const response = await this.connectionsApi.createConnection(
        projectName,
        connectionName,
        connection,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async updateConnection(
    projectName: string,
    connectionName: string,
    connection: any,
  ): Promise<any> {
    try {
      const response = await this.connectionsApi.updateConnection(
        projectName,
        connectionName,
        connection,
      );
      return response.data;
    } catch (error) {
      throw this.handleError(error as AxiosError);
    }
  }

  async deleteConnection(
    projectName: string,
    connectionName: string,
  ): Promise<void> {
    try {
      await this.connectionsApi.deleteConnection(projectName, connectionName);
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
