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

import io.branching.metastore.common.model.Database;
import org.jdbi.v3.json.Json;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * Data Access Object for database operations.
 * Branch-scoped following the branching architecture.
 */
public interface DatabaseDao extends SqlObject
{
    @SqlQuery("SELECT database_name, location, comment, properties " +
            "FROM databases " +
            "WHERE branch_id = :branchId AND database_name = :databaseName")
    Optional<Database> getDatabase(String branchId, String databaseName);

    @SqlQuery("SELECT database_name, location, comment, properties " +
            "FROM databases " +
            "WHERE branch_id = :branchId " +
            "ORDER BY database_name ASC")
    List<Database> listDatabases(String branchId);

    @SqlUpdate("INSERT INTO databases (branch_id, database_name, location, comment, properties) " +
            "VALUES (:branchId, :databaseName, :location, :comment, :properties)")
    void insertDatabase(String branchId, @Json Database database);

    @SqlUpdate("UPDATE databases SET " +
            "location = :location, " +
            "comment = :comment, " +
            "parameters = :properties " +
            "WHERE branch_id = :branchId AND database_name = :databaseName")
    void updateDatabase(String branchId, String databaseName, @Json Database database);

    @SqlUpdate("DELETE FROM databases " +
            "WHERE branch_id = :branchId AND database_name = :databaseName")
    boolean deleteDatabase(String branchId, String databaseName);

    @SqlQuery("SELECT COUNT(*) > 0 FROM databases " +
            "WHERE branch_id = :branchId AND database_name = :databaseName")
    boolean databaseExists(String branchId, String databaseName);

    @SqlQuery("SELECT database_name FROM databases " +
            "WHERE branch_id = :branchId AND database_name = :databaseName")
    Optional<String> getDatabaseLocation(String branchId, String databaseName);
}
