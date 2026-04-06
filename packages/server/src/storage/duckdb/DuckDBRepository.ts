import {
   Connection,
   ManifestEntry,
   Package,
   Project,
   ResourceRepository,
   Task,
   TaskExecution,
   TaskExecutionStatus,
} from "../DatabaseInterface";
import { ConnectionRepository } from "./ConnectionRepository";
import { DuckDBConnection } from "./DuckDBConnection";
import { ManifestRepository } from "./ManifestRepository";
import { PackageRepository } from "./PackageRepository";
import { ProjectRepository } from "./ProjectRepository";
import { TaskRepository } from "./TaskRepository";

export class DuckDBRepository implements ResourceRepository {
   private projectRepo: ProjectRepository;
   private packageRepo: PackageRepository;
   private connectionRepo: ConnectionRepository;
   private taskRepo: TaskRepository;
   private manifestRepo: ManifestRepository;

   constructor(public db: DuckDBConnection) {
      this.projectRepo = new ProjectRepository(db);
      this.packageRepo = new PackageRepository(db);
      this.connectionRepo = new ConnectionRepository(db);
      this.taskRepo = new TaskRepository(db);
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
      await this.taskRepo.deleteTasksByProjectId(id);
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

   // ==================== TASKS ====================

   async listTasks(projectId: string): Promise<Task[]> {
      return this.taskRepo.listTasks(projectId);
   }

   async getTaskById(id: string): Promise<Task | null> {
      return this.taskRepo.getTaskById(id);
   }

   async getTaskByName(projectId: string, name: string): Promise<Task | null> {
      return this.taskRepo.getTaskByName(projectId, name);
   }

   async createTask(
      task: Omit<Task, "id" | "createdAt" | "updatedAt">,
   ): Promise<Task> {
      return this.taskRepo.createTask(task);
   }

   async updateTask(id: string, updates: Partial<Task>): Promise<Task> {
      return this.taskRepo.updateTask(id, updates);
   }

   async deleteTask(id: string): Promise<void> {
      return this.taskRepo.deleteTask(id);
   }

   async deleteTasksByProjectId(projectId: string): Promise<void> {
      return this.taskRepo.deleteTasksByProjectId(projectId);
   }

   // ==================== TASK EXECUTIONS ====================

   async listExecutions(taskId: string): Promise<TaskExecution[]> {
      return this.taskRepo.listExecutions(taskId);
   }

   async getExecutionById(id: string): Promise<TaskExecution | null> {
      return this.taskRepo.getExecutionById(id);
   }

   async getRunningExecution(taskId: string): Promise<TaskExecution | null> {
      return this.taskRepo.getRunningExecution(taskId);
   }

   async createExecution(
      taskId: string,
      status: TaskExecutionStatus = "PENDING",
   ): Promise<TaskExecution | null> {
      return this.taskRepo.createExecution(taskId, status);
   }

   async updateExecution(
      id: string,
      updates: {
         status?: TaskExecutionStatus;
         startedAt?: Date;
         completedAt?: Date;
         error?: string | null;
         metadata?: Record<string, unknown> | null;
      },
   ): Promise<TaskExecution> {
      return this.taskRepo.updateExecution(id, updates);
   }

   // ==================== BUILD MANIFESTS ====================

   async listManifestEntries(
      projectId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      return this.manifestRepo.listEntries(projectId, packageName);
   }

   async getManifestEntryByBuildId(
      projectId: string,
      packageName: string,
      buildId: string,
   ): Promise<ManifestEntry | null> {
      return this.manifestRepo.getEntryByBuildId(
         projectId,
         packageName,
         buildId,
      );
   }

   async getManifestEntryBySourceName(
      projectId: string,
      packageName: string,
      sourceName: string,
   ): Promise<ManifestEntry | null> {
      return this.manifestRepo.getEntryBySourceName(
         projectId,
         packageName,
         sourceName,
      );
   }

   async upsertManifestEntry(
      entry: Omit<ManifestEntry, "id" | "createdAt" | "updatedAt">,
   ): Promise<ManifestEntry> {
      return this.manifestRepo.upsertEntry(entry);
   }

   async deleteManifestEntry(id: string): Promise<void> {
      return this.manifestRepo.deleteEntry(id);
   }

   async deleteManifestEntriesByPackage(
      projectId: string,
      packageName: string,
   ): Promise<void> {
      return this.manifestRepo.deleteEntriesByPackage(projectId, packageName);
   }

   async deleteManifestEntriesByProjectId(projectId: string): Promise<void> {
      return this.manifestRepo.deleteEntriesByProjectId(projectId);
   }
}
