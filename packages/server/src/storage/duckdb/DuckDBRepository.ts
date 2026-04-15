import {
   Connection,
   ManifestEntry,
   Materialization,
   MaterializationStatus,
   Package,
   Project,
   ResourceRepository,
} from "../DatabaseInterface";
import { ConnectionRepository } from "./ConnectionRepository";
import { DuckDBConnection } from "./DuckDBConnection";
import { ManifestRepository } from "./ManifestRepository";
import { MaterializationRepository } from "./MaterializationRepository";
import { PackageRepository } from "./PackageRepository";
import { ProjectRepository } from "./ProjectRepository";

export class DuckDBRepository implements ResourceRepository {
   private projectRepo: ProjectRepository;
   private packageRepo: PackageRepository;
   private connectionRepo: ConnectionRepository;
   private materializationRepo: MaterializationRepository;
   private manifestRepo: ManifestRepository;

   constructor(public db: DuckDBConnection) {
      this.projectRepo = new ProjectRepository(db);
      this.packageRepo = new PackageRepository(db);
      this.connectionRepo = new ConnectionRepository(db);
      this.materializationRepo = new MaterializationRepository(db);
      this.manifestRepo = new ManifestRepository(db);
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
      await this.manifestRepo.deleteEntriesByProjectId(id);
      await this.materializationRepo.deleteByProjectId(id);
      await this.connectionRepo.deleteConnectionsByProjectId(id);
      await this.packageRepo.deletePackagesByProjectId(id);
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
      const pkg = await this.packageRepo.getPackageById(id);
      if (pkg) {
         await this.manifestRepo.deleteEntriesByPackage(
            pkg.projectId,
            pkg.name,
         );
         await this.materializationRepo.deleteByPackage(
            pkg.projectId,
            pkg.name,
         );
      }
      await this.packageRepo.deletePackage(id);
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

   // ==================== MATERIALIZATIONS ====================

   async listMaterializations(
      projectId: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]> {
      return this.materializationRepo.list(projectId, packageName, options);
   }

   async getMaterializationById(id: string): Promise<Materialization | null> {
      return this.materializationRepo.getById(id);
   }

   async getActiveMaterialization(
      projectId: string,
      packageName: string,
   ): Promise<Materialization | null> {
      return this.materializationRepo.getActive(projectId, packageName);
   }

   async createMaterialization(
      projectId: string,
      packageName: string,
      status: MaterializationStatus = "PENDING",
   ): Promise<Materialization> {
      return this.materializationRepo.create(projectId, packageName, status);
   }

   async updateMaterialization(
      id: string,
      updates: {
         status?: MaterializationStatus;
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<Materialization> {
      return this.materializationRepo.update(id, updates);
   }

   async deleteMaterialization(id: string): Promise<void> {
      return this.materializationRepo.deleteById(id);
   }

   // ==================== BUILD MANIFESTS ====================

   async listManifestEntries(
      projectId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      return this.manifestRepo.listEntries(projectId, packageName);
   }

   async upsertManifestEntry(
      entry: Omit<ManifestEntry, "id" | "createdAt" | "updatedAt">,
   ): Promise<ManifestEntry> {
      return this.manifestRepo.upsertEntry(entry);
   }

   async deleteManifestEntry(id: string): Promise<void> {
      return this.manifestRepo.deleteEntry(id);
   }
}
