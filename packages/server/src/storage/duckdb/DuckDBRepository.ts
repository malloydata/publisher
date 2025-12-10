import {
   ResourceRepository,
   Project,
   Package,
   Connection,
} from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";
import { ProjectRepository } from "./ProjectRepository";
import { PackageRepository } from "./PackageRepository";
import { ConnectionRepository } from "./ConnectionRepository";

export class DuckDBRepository implements ResourceRepository {
   private projectRepo: ProjectRepository;
   private packageRepo: PackageRepository;
   private connectionRepo: ConnectionRepository;

   constructor(public db: DuckDBConnection) {
      this.projectRepo = new ProjectRepository(db);
      this.packageRepo = new PackageRepository(db);
      this.connectionRepo = new ConnectionRepository(db);
   }

   // ==================== PROJECTS ====================

   async listProjects(): Promise<Project[]> {
      return this.projectRepo.listProjects();
   }

   async getProjectById(id: string): Promise<Project | null> {
      return this.projectRepo.getProjectById(id);
   }

   async getProjectByName(name: string): Promise<Project | null> {
      return this.projectRepo.getProjectByName(name);
   }

   async createProject(
      project: Omit<Project, "id" | "createdAt" | "updatedAt">,
   ): Promise<Project> {
      return this.projectRepo.createProject(project);
   }

   async updateProject(
      id: string,
      updates: Partial<Project>,
   ): Promise<Project> {
      return this.projectRepo.updateProject(id, updates);
   }

   async deleteProject(id: string): Promise<void> {
      // Delete related connections and packages first
      await this.connectionRepo.deleteConnectionsByProjectId(id);
      await this.packageRepo.deletePackagesByProjectId(id);

      // Then delete the project
      await this.projectRepo.deleteProject(id);
   }

   // ==================== PACKAGES ====================

   async listPackages(projectId: string): Promise<Package[]> {
      return this.packageRepo.listPackages(projectId);
   }

   async getPackageById(id: string): Promise<Package | null> {
      return this.packageRepo.getPackageById(id);
   }

   async getPackageByName(
      projectId: string,
      name: string,
   ): Promise<Package | null> {
      return this.packageRepo.getPackageByName(projectId, name);
   }

   async createPackage(
      pkg: Omit<Package, "id" | "createdAt" | "updatedAt">,
   ): Promise<Package> {
      return this.packageRepo.createPackage(pkg);
   }

   async updatePackage(
      id: string,
      updates: Partial<Package>,
   ): Promise<Package> {
      return this.packageRepo.updatePackage(id, updates);
   }

   async deletePackage(id: string): Promise<void> {
      return this.packageRepo.deletePackage(id);
   }

   async deletePackagesByProjectId(id: string): Promise<void> {
      return this.packageRepo.deletePackagesByProjectId(id);
   }

   // ==================== CONNECTIONS ====================

   async listConnections(projectId: string): Promise<Connection[]> {
      return this.connectionRepo.listConnections(projectId);
   }

   async getConnectionById(id: string): Promise<Connection | null> {
      return this.connectionRepo.getConnectionById(id);
   }

   async getConnectionByName(
      projectId: string,
      name: string,
   ): Promise<Connection | null> {
      return this.connectionRepo.getConnectionByName(projectId, name);
   }

   async createConnection(
      connection: Omit<Connection, "id" | "createdAt" | "updatedAt">,
   ): Promise<Connection> {
      return this.connectionRepo.createConnection(connection);
   }

   async updateConnection(
      id: string,
      updates: Partial<Connection>,
   ): Promise<Connection> {
      return this.connectionRepo.updateConnection(id, updates);
   }

   async deleteConnection(id: string): Promise<void> {
      return this.connectionRepo.deleteConnection(id);
   }

   async deleteConnectionsByProjectId(id: string): Promise<void> {
      return this.connectionRepo.deleteConnectionsByProjectId(id);
   }
}
