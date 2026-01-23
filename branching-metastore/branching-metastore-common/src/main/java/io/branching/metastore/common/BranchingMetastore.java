/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.branching.metastore.common;

import io.branching.metastore.common.exceptions.BranchAlreadyExistsException;
import io.branching.metastore.common.exceptions.BranchNotFoundException;
import io.branching.metastore.common.exceptions.MergeConflictException;
import io.branching.metastore.common.exceptions.TableAlreadyExistsException;
import io.branching.metastore.common.exceptions.TableNotFoundException;
import io.branching.metastore.common.model.Branch;
import io.branching.metastore.common.model.BranchType;
import io.branching.metastore.common.model.Column;
import io.branching.metastore.common.model.Database;
import io.branching.metastore.common.model.Storage;
import io.branching.metastore.common.model.Table;
import io.branching.metastore.common.model.TableType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Main metastore interface for catalog-level branching.
 * Inspired by Nessie (2025-2026) and Stargate Metastore patterns.
 *
 * <p>Provides Git-like operations on metadata:
 * <ul>
 *   <li>Branch management (create, list, delete)</li>
 *   *li>Database operations (branch-scoped)</li>
   *   *li>Table operations (branch-scoped)</li>
 *   *<li>Merge operations</li>
 * </ul>
 */
public interface BranchingMetastore
{
    // ========== Branch Operations ==========

    /**
     * Get all branches in the metastore.
     */
    Collection<Branch> getAllBranches();

    /**
     * Get a specific branch by ID.
     */
    Optional<Branch> getBranch(String branchId)
            throws BranchNotFoundException;

    /**
     * Create a new branch.
     * @param branchId Unique branch identifier
     * @param parentId Optional parent branch for forking
     * @param branchType Type of branch (MAIN, FEATURE, RELEASE, SNAPSHOT)
     */
    void createBranch(String branchId, Optional<String> parentId, BranchType branchType)
            throws BranchAlreadyExistsException;

    /**
     * Delete a branch and all its metadata.
     */
    void deleteBranch(String branchId)
            throws BranchNotFoundException;

    /**
     * Merge source branch into target branch.
     * @param sourceBranch Source branch to merge from
     * @param targetBranch Target branch to merge into
     * @param commitMessage Merge commit message
     */
    void mergeBranch(String sourceBranch, String targetBranch, String commitMessage)
            throws MergeConflictException;

    // ========== Database Operations ==========

    /**
     * List all databases in a branch.
     */
    Collection<String> listDatabases(String branchId);

    /**
     * Get a specific database.
     */
    Optional<Database> getDatabase(String branchId, String databaseName);

    /**
     * Create a database in a branch.
     */
    void createDatabase(String branchId, Database database)
            throws BranchNotFoundException;

    /**
     * Drop a database from a branch.
     */
    void dropDatabase(String branchId, String databaseName)
            throws BranchNotFoundException;

    /**
     * Rename a database.
     */
    void renameDatabase(String branchId, String databaseName, String newDatabaseName)
            throws BranchNotFoundException;

    // ========== Table Operations ==========

    /**
     * List all tables in a database (branch-scoped).
     */
    Collection<TableInfo> listTables(String branchId, String databaseName);

    /**
     * Get a specific table.
     */
    Optional<Table> getTable(String branchId, String databaseName, String tableName)
            throws TableNotFoundException;

    /**
     * Create a table in a branch.
     */
    void createTable(String branchId, Table table)
            throws TableAlreadyExistsException;

    /**
     * Drop a table from a branch.
     */
    void dropTable(String branchId, String databaseName, String tableName)
            throws TableNotFoundException;

    /**
     * Replace a table (create or update).
     */
    void replaceTable(String branchId, Table table)
            throws TableNotFoundException;

    /**
     * Rename a table.
     */
    void renameTable(String branchId, String databaseName, String tableName,
                     String newDatabaseName, String newTableName)
            throws TableNotFoundException;

    /**
     * Add a column to a table.
     */
    void addColumn(String branchId, String databaseName, String tableName, Column column)
            throws TableNotFoundException;

    /**
     * Rename a column.
     */
    void renameColumn(String branchId, String databaseName, String tableName,
                      String columnName, String newColumnName)
            throws TableNotFoundException;

    /**
     * Drop a column from a table.
     */
    void dropColumn(String branchId, String databaseName, String tableName, String columnName)
            throws TableNotFoundException;

    /**
     * Get table names with pagination support.
     */
    GetTablesResult getTables(String branchId, String databaseName,
                             Optional<Integer> limit, Optional<String> nextToken);

    /**
     * Table info for listing operations.
     */
    record TableInfo(String databaseName, String tableName, TableType tableType)
    {
        public TableInfo(String databaseName, String tableName, TableType tableType)
        {
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.tableType = tableType;
        }

        public String databaseName() { return databaseName; }
        public String tableName() { return tableName; }
        public TableType tableType() { return tableType; }
    }

    /**
     * Paginated table listing result.
     */
    record GetTablesResult(List<TableInfo> tables, Optional<String> nextToken)
    {
        public GetTablesResult(List<TableInfo> tables, Optional<String> nextToken)
        {
            this.tables = tables;
            this.nextToken = nextToken;
        }

        public List<TableInfo> tables() { return tables; }
        public Optional<String> nextToken() { return nextToken; }
    }
}
