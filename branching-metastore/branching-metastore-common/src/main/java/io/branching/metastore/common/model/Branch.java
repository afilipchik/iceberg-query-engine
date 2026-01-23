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
package io.branching.metastore.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Map;
import java.util.Optional;

/**
 * Branch metadata representing a isolated branch in the metastore.
 * Based on Nessie's branch concept (Git-like branching for data).
 *
 * <p>References:
 * <ul>
 *   <li>Nessie Catalog (2025) - Git-like branching for Iceberg</li>
 *   <li>lakeFS Data Version Control (2026) - Branch isolation patterns</li>
 * </ul>
 */
@Immutable
public class Branch
{
    private final String branchId;
    private final Optional<String> parentId;
    private final Optional<String> headSnapshotId;
    private final BranchType branchType;
    private final Optional<String> comment;
    private final Map<String, String> properties;

    @JsonCreator
    public Branch(
            @JsonProperty("branchId") String branchId,
            @JsonProperty("parentId") Optional<String> parentId,
            @JsonProperty("headSnapshotId") Optional<String> headSnapshotId,
            @JsonProperty("branchType") BranchType branchType,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.branchId = branchId;
        this.parentId = parentId;
        this.headSnapshotId = headSnapshotId;
        this.branchType = branchType;
        this.comment = comment;
        this.properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @JsonProperty
    public String branchId()
    {
        return branchId;
    }

    @JsonProperty
    public Optional<String> parentId()
    {
        return parentId;
    }

    @JsonProperty
    public Optional<String> headSnapshotId()
    {
        return headSnapshotId;
    }

    @JsonProperty
    public BranchType branchType()
    {
        return branchType;
    }

    @JsonProperty
    public Optional<String> comment()
    {
        return comment;
    }

    @JsonProperty
    public Map<String, String> properties()
    {
        return properties;
    }

    public Builder toBuilder()
    {
        return new Builder()
                .setBranchId(branchId)
                .setParentId(parentId.orElse(null))
                .setHeadSnapshotId(headSnapshotId.orElse(null))
                .setBranchType(branchType)
                .setComment(comment.orElse(null))
                .setProperties(properties);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String branchId;
        private String parentId;
        private String headSnapshotId;
        private BranchType branchType;
        private String comment;
        private Map<String, String> properties = Map.of();

        public Builder setBranchId(String branchId)
        {
            this.branchId = branchId;
            return this;
        }

        public Builder setParentId(String parentId)
        {
            this.parentId = parentId;
            return this;
        }

        public Builder setHeadSnapshotId(String headSnapshotId)
        {
            this.headSnapshotId = headSnapshotId;
            return this;
        }

        public Builder setBranchType(BranchType branchType)
        {
            this.branchType = branchType;
            return this;
        }

        public Builder setComment(String comment)
        {
            this.comment = comment;
            return this;
        }

        public Builder setProperties(Map<String, String> properties)
        {
            this.properties = properties == null ? Map.of() : properties;
            return this;
        }

        public Branch build()
        {
            return new Branch(
                    branchId,
                    Optional.ofNullable(parentId),
                    Optional.ofNullable(headSnapshotId),
                    branchType,
                    Optional.ofNullable(comment),
                    properties);
        }
    }
}
