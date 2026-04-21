import {
   Connection,
   Environment,
   ManifestEntry,
   Materialization,
   MaterializationStatus,
   Package,
   ResourceRepository,
} from "../DatabaseInterface";
import { ConnectionRepository } from "./ConnectionRepository";
import { DuckDBConnection } from "./DuckDBConnection";
import { EnvironmentRepository } from "./EnvironmentRepository";
import { ManifestRepository } from "./ManifestRepository";
import { MaterializationRepository } from "./MaterializationRepository";
import { PackageRepository } from "./PackageRepository";

export class DuckDBRepository implements ResourceRepository {
   private environmentRepo: EnvironmentRepository;
   private packageRepo: PackageRepository;
   private connectionRepo: ConnectionRepository;
   private materializationRepo: MaterializationRepository;
   private manifestRepo: ManifestRepository;

   constructor(public db: DuckDBConnection) {
      this.environmentRepo = new EnvironmentRepository(db);
      this.packageRepo = new PackageRepository(db);
      this.connectionRepo = new ConnectionRepository(db);
      this.materializationRepo = new MaterializationRepository(db);
      this.manifestRepo = new ManifestRepository(db);
   }

   // ==================== ENVIRONMENTS ====================

   async listEnvironments(): Promise<Environment[]> {
      return this.environmentRepo.listEnvironments();
   }

   async getEnvironmentById(id: string): Promise<Environment | null> {
      return this.environmentRepo.getEnvironmentById(id);
   }

   async getEnvironmentByName(name: string): Promise<Environment | null> {
      return this.environmentRepo.getEnvironmentByName(name);
   }

   async createEnvironment(
      environment: Omit<Environment, "id" | "createdAt" | "updatedAt">,
   ): Promise<Environment> {
      return this.environmentRepo.createEnvironment(environment);
   }

   async updateEnvironment(
      id: string,
      updates: Partial<Environment>,
   ): Promise<Environment> {
      return this.environmentRepo.updateEnvironment(id, updates);
   }

   async deleteEnvironment(id: string): Promise<void> {
      await this.manifestRepo.deleteEntriesByEnvironmentId(id);
      await this.materializationRepo.deleteByEnvironmentId(id);
      await this.connectionRepo.deleteConnectionsByEnvironmentId(id);
      await this.packageRepo.deletePackagesByEnvironmentId(id);
      await this.environmentRepo.deleteEnvironment(id);
   }

   // ==================== PACKAGES ====================

   async listPackages(environmentId: string): Promise<Package[]> {
      return this.packageRepo.listPackages(environmentId);
   }

   async getPackageById(id: string): Promise<Package | null> {
      return this.packageRepo.getPackageById(id);
   }

   async getPackageByName(
      environmentId: string,
      name: string,
   ): Promise<Package | null> {
      return this.packageRepo.getPackageByName(environmentId, name);
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
            pkg.environmentId,
            pkg.name,
         );
         await this.materializationRepo.deleteByPackage(
            pkg.environmentId,
            pkg.name,
         );
      }
      await this.packageRepo.deletePackage(id);
   }

   async deletePackagesByEnvironmentId(id: string): Promise<void> {
      return this.packageRepo.deletePackagesByEnvironmentId(id);
   }

   // ==================== CONNECTIONS ====================

   async listConnections(environmentId: string): Promise<Connection[]> {
      return this.connectionRepo.listConnections(environmentId);
   }

   async getConnectionById(id: string): Promise<Connection | null> {
      return this.connectionRepo.getConnectionById(id);
   }

   async getConnectionByName(
      environmentId: string,
      name: string,
   ): Promise<Connection | null> {
      return this.connectionRepo.getConnectionByName(environmentId, name);
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

   async deleteConnectionsByEnvironmentId(id: string): Promise<void> {
      return this.connectionRepo.deleteConnectionsByEnvironmentId(id);
   }

   // ==================== MATERIALIZATIONS ====================

   async listMaterializations(
      environmentId: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]> {
      return this.materializationRepo.list(environmentId, packageName, options);
   }

   async getMaterializationById(id: string): Promise<Materialization | null> {
      return this.materializationRepo.getById(id);
   }

   async getActiveMaterialization(
      environmentId: string,
      packageName: string,
   ): Promise<Materialization | null> {
      return this.materializationRepo.getActive(environmentId, packageName);
   }

   async createMaterialization(
      environmentId: string,
      packageName: string,
      status: MaterializationStatus = "PENDING",
      metadata: Record<string, unknown> | null = null,
   ): Promise<Materialization> {
      return this.materializationRepo.create(
         environmentId,
         packageName,
         status,
         metadata,
      );
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
      environmentId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      return this.manifestRepo.listEntries(environmentId, packageName);
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
