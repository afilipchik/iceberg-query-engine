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
package io.branching.metastore.server.dao;

import io.branching.metastore.common.model.Column;
import io.branching.metastore.common.model.Storage;
import io.branching.metastore.common.model.Table;
import io.branching.metastore.common.model.TableType;
import org.jdbi.v3.json.Json;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Data Access Object for table operations.
 * Branch-scoped following the branching architecture.
 */
public interface TableDao extends SqlObject
{
    @SqlQuery("SELECT database_name, table_name, table_type, storage, " +
            "data_columns, partition_columns, parameters, " +
            "view_original_text, view_expanded_text " +
            "FROM tables " +
            "WHERE branch_id = :branchId AND database_name = :databaseName AND table_name = :tableName")
    Optional<Table> getTable(String branchId, String databaseName, String tableName);

    @SqlQuery("SELECT database_name, table_name, table_type, storage, " +
            "data_columns, partition_columns, parameters, " +
            "view_original_text, view_expanded_text " +
            "FROM tables " +
            "WHERE branch_id = :branchId AND database_name = :databaseName " +
            "ORDER BY table_name ASC " +
            "LIMIT :limit " +
            "OFFSET :offset")
    List<Table> listTables(String branchId, String databaseName, int limit, int offset);

    @SqlQuery("SELECT COUNT(*) FROM tables " +
            "WHERE branch_id = :branchId AND database_name = :databaseName")
    int countTables(String branchId, String databaseName);

    @SqlUpdate("INSERT INTO tables (" +
            "branch_id, database_name, table_name, table_type, " +
            "storage, data_columns, partition_columns, parameters, " +
            "view_original_text, view_expanded_text) " +
            "VALUES (:branchId, :databaseName, :tableName, :tableType, " +
            ":storage, :dataColumns, :partitionColumns, :parameters, " +
            ":viewOriginalText, :viewExpandedText)")
    void insertTable(String branchId, @Json Table table);

    @SqlUpdate("UPDATE tables SET " +
            "table_type = :tableType, " +
            "storage = :storage, " +
            "data_columns = :dataColumns, " +
            "partition_columns = :partitionColumns, " +
            "parameters = :parameters, " +
            "view_original_text = :viewOriginalText, " +
            "view_expanded_text = :viewExpandedText, " +
            "updated_at = CURRENT_TIMESTAMP " +
            "WHERE branch_id = :branchId AND database_name = :databaseName AND table_name = :tableName")
    void updateTable(String branchId, @Json Table table);

    @SqlUpdate("DELETE FROM tables " +
            "WHERE branch_id = :branchId AND database_name = :databaseName AND table_name = :tableName")
    boolean deleteTable(String branchId, String databaseName, String tableName);

    @SqlUpdate("UPDATE tables SET " +
            "database_name = :newDatabaseName, " +
            "table_name = :newTableName, " +
            "updated_at = CURRENT_TIMESTAMP " +
            "WHERE branch_id = :branchId AND database_name = :databaseName AND table_name = :tableName")
    boolean renameTable(String branchId, String databaseName, String tableName,
                      String newDatabaseName, String newTableName);

    @SqlQuery("SELECT COUNT(*) > 0 FROM tables " +
            "WHERE branch_id = :branchId AND database_name = :databaseName AND table_name = :tableName")
    boolean tableExists(String branchId, String databaseName, String tableName);

    @SqlQuery("SELECT table_name FROM tables " +
            "WHERE branch_id = :branchId AND database_name = :databaseName " +
            "ORDER BY table_name ASC " +
            "LIMIT :limit " +
            "OFFSET :offset")
    List<String> getTableNames(String branchId, String databaseName, int limit, int offset);
}
