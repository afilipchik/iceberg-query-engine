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

import io.branching.metastore.common.exceptions.BranchAlreadyExistsException;
import io.branching.metastore.common.model.Branch;
import io.branching.metastore.common.model.BranchType;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

/**
 * Data Access Object for branch operations.
 * Follows JDBI pattern from Stargate MetastoreDao (2025-2026).
 */
public interface BranchDao extends SqlObject
{
    @SqlQuery("SELECT branch_id, parent_id, head_snapshot_id, branch_type, comment, properties " +
            "FROM branches WHERE branch_id = :branchId")
    Optional<Branch> getBranch(String branchId);

    @SqlQuery("SELECT branch_id, parent_id, head_snapshot_id, branch_type, comment, properties " +
            "FROM branches ORDER BY created_at DESC")
    List<Branch> listAllBranches();

    @SqlQuery("SELECT branch_id, parent_id, head_snapshot_id, branch_type, comment, properties " +
            "FROM branches WHERE branch_type = :branchType ORDER BY created_at DESC")
    List<Branch> listBranchesByType(BranchType branchType);

    @SqlUpdate("INSERT INTO branches (branch_id, parent_id, head_snapshot_id, branch_type, comment, properties) " +
            "VALUES (:branchId, :parentId, :headSnapshotId, :branchType, :comment, :properties)")
    void insertBranch(Branch branch);

    @SqlUpdate("UPDATE branches SET " +
            "parent_id = :parentId, " +
            "head_snapshot_id = :headSnapshotId, " +
            "branch_type = :branchType, " +
            "comment = :comment, " +
            "properties = :properties, " +
            "updated_at = CURRENT_TIMESTAMP " +
            "WHERE branch_id = :branchId")
    void updateBranch(Branch branch);

    @SqlUpdate("DELETE FROM branches WHERE branch_id = :branchId")
    boolean deleteBranch(String branchId);

    @SqlQuery("SELECT COUNT(*) > 0 FROM branches WHERE branch_id = :branchId")
    boolean branchExists(String branchId);

    @SqlUpdate("DELETE FROM branches WHERE parent_id = :branchId")
    int deleteChildBranches(String branchId);
}
