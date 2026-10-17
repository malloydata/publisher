// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { ResourceRepository } from "../DatabaseInterface";
import { ConnectionRepository } from "./ConnectionRepository";
import { DuckDBConnection } from "./DuckDBConnection";
import { EnvironmentRepository } from "./EnvironmentRepository";
import { IncrementalLedgerRepository } from "./IncrementalLedgerRepository";
import { StorageDestinationRepository } from "./StorageDestinationRepository";
import { MaterializationRepository } from "./MaterializationRepository";
import { PackageRepository } from "./PackageRepository";
import { VersionRepository } from "./VersionRepository";

/**
 * The {@link ResourceRepository} over one DuckDB store: each method delegates
 * to the entity repository that owns its table, except the deletes that
 * cascade across tables.
 */
export function createDuckDBRepository(
   db: DuckDBConnection,
): ResourceRepository {
   const environments = new EnvironmentRepository(db);
   const packages = new PackageRepository(db);
   const versions = new VersionRepository(db);
   const connections = new ConnectionRepository(db);
   const destinations = new StorageDestinationRepository(db);
   const materializations = new MaterializationRepository(db);
   const ledger = new IncrementalLedgerRepository(db);

   return {
      listEnvironments: () => environments.listEnvironments(),
      getEnvironmentById: (id) => environments.getEnvironmentById(id),
      getEnvironmentByName: (name) => environments.getEnvironmentByName(name),
      createEnvironment: (environment) =>
         environments.createEnvironment(environment),
      updateEnvironment: (id, updates) =>
         environments.updateEnvironment(id, updates),
      async deleteEnvironment(id) {
         await ledger.deleteByEnvironmentId(id);
         await materializations.deleteByEnvironmentId(id);
         await connections.deleteConnectionsByEnvironmentId(id);
         await destinations.deleteByEnvironmentId(id);
         await versions.deleteByEnvironmentId(id);
         await packages.deletePackagesByEnvironmentId(id);
         await environments.deleteEnvironment(id);
      },

      listPackages: (environmentId) => packages.listPackages(environmentId),
      getPackageById: (id) => packages.getPackageById(id),
      getPackageByName: (environmentId, name) =>
         packages.getPackageByName(environmentId, name),
      createPackage: (pkg) => packages.createPackage(pkg),
      updatePackage: (id, updates) => packages.updatePackage(id, updates),
      async deletePackage(id) {
         const pkg = await packages.getPackageById(id);
         if (pkg) {
            await ledger.deleteByPackage(pkg.environmentId, pkg.name);
            await materializations.deleteByPackage(pkg.environmentId, pkg.name);
            await versions.deleteByPackage(pkg.environmentId, pkg.name);
         }
         await packages.deletePackage(id);
      },
      deletePackageRecord: (id) => packages.deletePackage(id),

      listVersions: (environmentId, packageName) =>
         versions.list(environmentId, packageName),
      hasVersions: (environmentId, packageName) =>
         versions.hasAny(environmentId, packageName),
      listVersionsByEnvironment: (environmentId) =>
         versions.listByEnvironment(environmentId),
      getVersion: (environmentId, packageName, versionId) =>
         versions.get(environmentId, packageName, versionId),
      commitPublish: (version, promote) =>
         versions.commitPublish(version, promote),
      setLatestVersion: (environmentId, packageName, versionId, onlyIf) =>
         versions.setLatest(environmentId, packageName, versionId, onlyIf),
      setVersionArchiveStatus: (
         environmentId,
         packageName,
         versionId,
         status,
      ) =>
         versions.setArchiveStatus(
            environmentId,
            packageName,
            versionId,
            status,
         ),
      setVersionManifestPath: (
         environmentId,
         packageName,
         versionId,
         manifestPath,
      ) =>
         versions.setManifestPath(
            environmentId,
            packageName,
            versionId,
            manifestPath,
         ),

      listConnections: (environmentId) =>
         connections.listConnections(environmentId),
      getConnectionById: (id) => connections.getConnectionById(id),
      getConnectionByName: (environmentId, name) =>
         connections.getConnectionByName(environmentId, name),
      createConnection: (connection) =>
         connections.createConnection(connection),
      updateConnection: (id, updates) =>
         connections.updateConnection(id, updates),
      deleteConnection: (id) => connections.deleteConnection(id),

      listStorageDestinations: (environmentId) =>
         destinations.list(environmentId),
      getStorageDestinationByName: (environmentId, name) =>
         destinations.getByName(environmentId, name),
      upsertStorageDestination: (destination) =>
         destinations.upsert(destination),
      deleteStorageDestination: (id) => destinations.deleteById(id),
      deleteStorageDestinationsByEnvironmentId: (id) =>
         destinations.deleteByEnvironmentId(id),

      listMaterializations: (environmentId, packageName, options) =>
         materializations.list(environmentId, packageName, options),
      getLatestScheduledFireAt: (environmentId, packageName, version) =>
         materializations.getLatestScheduledFireAt(
            environmentId,
            packageName,
            version,
         ),
      getMaterializationById: (id) => materializations.getById(id),
      getActiveMaterialization: (environmentId, packageName, lockVersion) =>
         materializations.getActive(environmentId, packageName, lockVersion),
      createMaterialization: (
         environmentId,
         packageName,
         status,
         metadata,
         options,
      ) =>
         materializations.create(
            environmentId,
            packageName,
            status,
            metadata,
            options,
         ),
      updateMaterialization: (id, updates) =>
         materializations.update(id, updates),
      deleteMaterialization: (id) => materializations.deleteById(id),

      getIncrementalLedgerEntry: (environmentId, table) =>
         ledger.get(environmentId, table),
      upsertIncrementalLedgerEntry: (entry) => ledger.upsert(entry),
      deleteIncrementalLedgerEntry: (environmentId, table) =>
         ledger.deleteEntry(environmentId, table),
   };
}
