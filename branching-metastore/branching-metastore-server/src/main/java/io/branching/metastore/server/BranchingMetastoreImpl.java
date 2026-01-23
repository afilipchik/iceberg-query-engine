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
package io.branching.metastore.server;

import io.branching.metastore.common.BranchingMetastore;
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
import io.branching.metastore.server.dao.BranchDao;
import io.branching.metastore.server.dao.DatabaseDao;
import io.branching.metastore.server.dao.TableDao;
import jakarta.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.json.Json;

import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Main branching metastore implementation.
 * Git-like operations on metadata with branch-scoped isolation.
 */
public class BranchingMetastoreImpl
        implements BranchingMetastore
{
    private final Jdbi jdbi;

    @Inject
    public BranchingMetastoreImpl(Jdbi jdbi)
    {
        this.jdbi = requireNonNull(jdbi, "jdbi is null");
    }

    // ========== Branch Operations ==========

    @Override
    public Collection<Branch> getAllBranches()
    {
        return jdbi.withExtension(BranchDao.class, dao -> dao.listAllBranches());
    }

    @Override
    public Optional<Branch> getBranch(String branchId)
            throws BranchNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        return jdbi.withExtension(BranchDao.class, dao -> dao.getBranch(branchId));
    }

    @Override
    public void createBranch(String branchId, Optional<String> parentId, BranchType branchType)
            throws BranchAlreadyExistsException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(parentId, "parentId is null");
        requireNonNull(branchType, "branchType is null");

        jdbi.useExtension(BranchDao.class, dao -> {
            if (dao.branchExists(branchId)) {
                throw new BranchAlreadyExistsException("Branch already exists: " + branchId);
            }

            if (parentId.isPresent() && !dao.branchExists(parentId.get())) {
                throw new BranchNotFoundException("Parent branch not found: " + parentId.get());
            }

            Branch branch = Branch.builder()
                    .setBranchId(branchId)
                    .setParentId(parentId.orElse(null))
                    .setHeadSnapshotId(null)
                    .setBranchType(branchType)
                    .setComment("Created branch: " + branchId)
                    .build();

            dao.insertBranch(branch);
        });
    }

    @Override
    public void deleteBranch(String branchId)
            throws BranchNotFoundException
    {
        requireNonNull(branchId, "branchId is null");

        jdbi.useExtension(BranchDao.class, dao -> {
            if (!dao.branchExists(branchId)) {
                throw new BranchNotFoundException("Branch not found: " + branchId);
            }

            // Delete child branches first
            dao.deleteChildBranches(branchId);

            // Delete the branch
            dao.deleteBranch(branchId);
        });
    }

    @Override
    public void mergeBranch(String sourceBranch, String targetBranch, String commitMessage)
            throws MergeConflictException
    {
        requireNonNull(sourceBranch, "sourceBranch is null");
        requireNonNull(targetBranch, "targetBranch is null");
        requireNonNull(commitMessage, "commitMessage is null");

        jdbi.useTransaction(handle -> {
            BranchDao branchDao = handle.attach(BranchDao.class);
            TableDao tableDao = handle.attach(TableDao.class);
            DatabaseDao databaseDao = handle.attach(DatabaseDao.class);

            // Verify branches exist
            if (!branchDao.branchExists(sourceBranch)) {
                throw new BranchNotFoundException("Source branch not found: " + sourceBranch);
            }
            if (!branchDao.branchExists(targetBranch)) {
                throw new BranchNotFoundException("Target branch not found: " + targetBranch);
            }

            // Collect all databases and tables from source branch
            List<Database> sourceDatabases = databaseDao.listDatabases(sourceBranch);

            // Check for conflicts in target branch
            for (Database database : sourceDatabases) {
                List<String> sourceTableNames = handle.select(
                        "SELECT table_name FROM tables WHERE branch_id = ? AND database_name = ?",
                        sourceBranch, database.databaseName())
                        .mapTo(String.class)
                        .list();

                List<String> targetTableNames = handle.select(
                        "SELECT table_name FROM tables WHERE branch_id = ? AND database_name = ?",
                        targetBranch, database.databaseName())
                        .mapTo(String.class)
                        .list();

                // Check for table name conflicts with different schemas
                for (String tableName : sourceTableNames) {
                    if (targetTableNames.contains(tableName)) {
                        Table sourceTable = tableDao.getTable(sourceBranch, database.databaseName(), tableName)
                                .orElseThrow();

                        Table targetTable = tableDao.getTable(targetBranch, database.databaseName(), tableName)
                                .orElseThrow();

                        // Simple conflict detection: different table types or schemas
                        if (!sourceTable.tableType().equals(targetTable.tableType()) ||
                                !schemasEqual(sourceTable, targetTable)) {
                            throw new MergeConflictException(
                                    "Merge conflict: table " + database.databaseName() + "." + tableName +
                                            " has diverged between branches");
                        }
                    }
                }
            }

            // Perform merge: copy/update databases and tables
            for (Database database : sourceDatabases) {
                Optional<Database> existingDb = databaseDao.getDatabase(targetBranch, database.databaseName());
                if (existingDb.isEmpty()) {
                    databaseDao.insertDatabase(targetBranch, database);
                }
            }

            // Copy tables from source to target (upsert)
            for (Database database : sourceDatabases) {
                List<String> sourceTableNames = handle.select(
                        "SELECT table_name FROM tables WHERE branch_id = ? AND database_name = ?",
                        sourceBranch, database.databaseName())
                        .mapTo(String.class)
                        .list();

                for (String tableName : sourceTableNames) {
                    // Get table from source branch using DAO
                    Table sourceTable = tableDao.getTable(sourceBranch, database.databaseName(), tableName)
                            .orElseThrow();

                    // Update or insert table in target branch
                    // Use updateTable for upsert (will update if exists, insert if not)
                    tableDao.updateTable(targetBranch, sourceTable);
                }
            }
        });
    }

    private boolean schemasEqual(Table t1, Table t2)
    {
        return t1.dataColumns().equals(t2.dataColumns()) &&
                t1.partitionColumns().equals(t2.partitionColumns());
    }

    // ========== Database Operations ==========

    @Override
    public Collection<String> listDatabases(String branchId)
    {
        requireNonNull(branchId, "branchId is null");
        return jdbi.withExtension(DatabaseDao.class, dao ->
                dao.listDatabases(branchId).stream()
                        .map(Database::databaseName)
                        .toList());
    }

    @Override
    public Optional<Database> getDatabase(String branchId, String databaseName)
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");
        return jdbi.withExtension(DatabaseDao.class, dao ->
                dao.getDatabase(branchId, databaseName));
    }

    @Override
    public void createDatabase(String branchId, Database database)
            throws BranchNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(database, "database is null");

        jdbi.useExtension(DatabaseDao.class, dao -> {
            if (!jdbi.withExtension(BranchDao.class, bDao -> bDao.branchExists(branchId))) {
                throw new BranchNotFoundException("Branch not found: " + branchId);
            }

            if (dao.databaseExists(branchId, database.databaseName())) {
                // Already exists, treat as success (idempotent)
                return;
            }

            dao.insertDatabase(branchId, database);
        });
    }

    @Override
    public void dropDatabase(String branchId, String databaseName)
            throws BranchNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");

        jdbi.useExtension(DatabaseDao.class, dao -> {
            if (!jdbi.withExtension(BranchDao.class, bDao -> bDao.branchExists(branchId))) {
                throw new BranchNotFoundException("Branch not found: " + branchId);
            }

            dao.deleteDatabase(branchId, databaseName);
        });
    }

    @Override
    public void renameDatabase(String branchId, String databaseName, String newDatabaseName)
            throws BranchNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");

        jdbi.useExtension(DatabaseDao.class, dao -> {
            if (!jdbi.withExtension(BranchDao.class, bDao -> bDao.branchExists(branchId))) {
                throw new BranchNotFoundException("Branch not found: " + branchId);
            }

            dao.updateDatabase(branchId, newDatabaseName, dao.getDatabase(branchId, databaseName).orElseThrow());
        });
    }

    // ========== Table Operations ==========

    @Override
    public Collection<TableInfo> listTables(String branchId, String databaseName)
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");

        return jdbi.withExtension(TableDao.class, dao -> {
            List<Table> tables = dao.listTables(branchId, databaseName, Integer.MAX_VALUE, 0);
            return tables.stream()
                    .map(t -> new TableInfo(t.databaseName(), t.tableName(), t.tableType()))
                    .toList();
        });
    }

    @Override
    public Optional<Table> getTable(String branchId, String databaseName, String tableName)
            throws TableNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        return jdbi.withExtension(TableDao.class, dao ->
                dao.getTable(branchId, databaseName, tableName));
    }

    @Override
    public void createTable(String branchId, Table table)
            throws TableAlreadyExistsException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(table, "table is null");

        jdbi.useExtension(TableDao.class, dao -> {
            if (dao.tableExists(branchId, table.databaseName(), table.tableName())) {
                throw new TableAlreadyExistsException(
                        "Table already exists: " + table.databaseName() + "." + table.tableName());
            }

            dao.insertTable(branchId, table);
        });
    }

    @Override
    public void dropTable(String branchId, String databaseName, String tableName)
            throws TableNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        jdbi.useExtension(TableDao.class, dao ->
                dao.deleteTable(branchId, databaseName, tableName));
    }

    @Override
    public void replaceTable(String branchId, Table table)
            throws TableNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(table, "table is null");

        jdbi.useExtension(TableDao.class, dao -> {
            if (dao.tableExists(branchId, table.databaseName(), table.tableName())) {
                dao.updateTable(branchId, table);
            }
            else {
                dao.insertTable(branchId, table);
            }
        });
    }

    @Override
    public void renameTable(String branchId, String databaseName, String tableName,
                            String newDatabaseName, String newTableName)
            throws TableNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");
        requireNonNull(newTableName, "newTableName is null");

        jdbi.useExtension(TableDao.class, dao ->
                dao.renameTable(branchId, databaseName, tableName, newDatabaseName, newTableName));
    }

    @Override
    public void addColumn(String branchId, String databaseName, String tableName, Column column)
            throws TableNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(column, "column is null");

        jdbi.withExtension(TableDao.class, dao -> {
            Table table = dao.getTable(branchId, databaseName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(
                            "Table not found: " + databaseName + "." + tableName));

            List<Column> newDataColumns = new java.util.ArrayList<>(table.dataColumns());
            newDataColumns.add(column);

            Table updatedTable = table.toBuilder()
                    .setDataColumns(newDataColumns)
                    .build();

            dao.updateTable(branchId, updatedTable);
            return null;
        });
    }

    @Override
    public void renameColumn(String branchId, String databaseName, String tableName,
                             String columnName, String newColumnName)
            throws TableNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnName, "columnName is null");
        requireNonNull(newColumnName, "newColumnName is null");

        jdbi.withExtension(TableDao.class, dao -> {
            Table table = dao.getTable(branchId, databaseName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(
                            "Table not found: " + databaseName + "." + tableName));

            List<Column> newDataColumns = table.dataColumns().stream()
                    .map(c -> c.name().equals(columnName) ?
                            new Column(newColumnName, c.type(), c.comment()) : c)
                    .toList();

            Table updatedTable = table.toBuilder()
                    .setDataColumns(newDataColumns)
                    .build();

            dao.updateTable(branchId, updatedTable);
            return null;
        });
    }

    @Override
    public void dropColumn(String branchId, String databaseName, String tableName, String columnName)
            throws TableNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnName, "columnName is null");

        jdbi.withExtension(TableDao.class, dao -> {
            Table table = dao.getTable(branchId, databaseName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(
                            "Table not found: " + databaseName + "." + tableName));

            List<Column> newDataColumns = table.dataColumns().stream()
                    .filter(c -> !c.name().equals(columnName))
                    .toList();

            Table updatedTable = table.toBuilder()
                    .setDataColumns(newDataColumns)
                    .build();

            dao.updateTable(branchId, updatedTable);
            return null;
        });
    }

    @Override
    public GetTablesResult getTables(String branchId, String databaseName,
                                     Optional<Integer> limit, Optional<String> nextToken)
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(nextToken, "nextToken is null");

        int actualLimit = limit.orElse(100);
        int offset = nextToken.map(s -> {
            try {
                return Integer.parseInt(new String(Base64.getDecoder().decode(s)));
            }
            catch (IllegalArgumentException e) {
                return 0;
            }
        }).orElse(0);

        List<Table> tables = jdbi.withExtension(TableDao.class, dao ->
                dao.listTables(branchId, databaseName, actualLimit + 1, offset));

        boolean hasMore = tables.size() > actualLimit;
        List<Table> resultTables = hasMore ? tables.subList(0, actualLimit) : tables;

        List<TableInfo> tableInfos = resultTables.stream()
                .map(t -> new TableInfo(t.databaseName(), t.tableName(), t.tableType()))
                .toList();

        Optional<String> newNextToken = Optional.empty();
        if (hasMore) {
            newNextToken = Optional.of(Base64.getEncoder().encodeToString(
                    String.valueOf(offset + actualLimit).getBytes()));
        }

        return new GetTablesResult(tableInfos, newNextToken);
    }
}
