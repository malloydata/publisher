import { AxiosError } from "axios";
import { logAxiosError } from "../utils/logger.js";
import {
  Configuration,
  ConnectionsApi,
  EnvironmentsApi,
  PackagesApi,
} from "./generated";

export class PublisherClient {
  private environmentsApi: EnvironmentsApi;
  private packagesApi: PackagesApi;
  private connectionsApi: ConnectionsApi;
  private baseURL: string;

  constructor(urlOverride?: string) {
    this.baseURL = this.resolveServerURL(urlOverride);

    const config = new Configuration({
      basePath: `${this.baseURL}/api/v0`,
    });

    this.environmentsApi = new EnvironmentsApi(config);
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
