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

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Table metadata, branch-scoped following the branching architecture.
 * JSON storage for schema matches Stargate and Iceberg patterns (2025-2026).
 */
@Immutable
public class Table
{
    private final String databaseName;
    private final String tableName;
    private final TableType tableType;
    private final Storage storage;
    private final List<Column> dataColumns;
    private final List<Column> partitionColumns;
    private final Map<String, String> parameters;
    private final Optional<String> viewOriginalText;
    private final Optional<String> viewExpandedText;

    @JsonCreator
    public Table(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") TableType tableType,
            @JsonProperty("storage") Storage storage,
            @JsonProperty("dataColumns") List<Column> dataColumns,
            @JsonProperty("partitionColumns") List<Column> partitionColumns,
            @JsonProperty("parameters") Map<String, String> parameters,
            @JsonProperty("viewOriginalText") Optional<String> viewOriginalText,
            @JsonProperty("viewExpandedText") Optional<String> viewExpandedText)
    {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableType = tableType;
        this.storage = storage;
        this.dataColumns = dataColumns == null ? List.of() : List.copyOf(dataColumns);
        this.partitionColumns = partitionColumns == null ? List.of() : List.copyOf(partitionColumns);
        this.parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
        this.viewOriginalText = viewOriginalText;
        this.viewExpandedText = viewExpandedText;
    }

    @JsonProperty
    public String databaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String tableName()
    {
        return tableName;
    }

    @JsonProperty
    public TableType tableType()
    {
        return tableType;
    }

    @JsonProperty
    public Storage storage()
    {
        return storage;
    }

    @JsonProperty
    public List<Column> dataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public List<Column> partitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public Map<String, String> parameters()
    {
        return parameters;
    }

    @JsonProperty
    public Optional<String> viewOriginalText()
    {
        return viewOriginalText;
    }

    @JsonProperty
    public Optional<String> viewExpandedText()
    {
        return viewExpandedText;
    }

    public Builder toBuilder()
    {
        return new Builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setTableType(tableType)
                .setStorage(storage)
                .setDataColumns(dataColumns)
                .setPartitionColumns(partitionColumns)
                .setParameters(parameters)
                .setViewOriginalText(viewOriginalText.orElse(null))
                .setViewExpandedText(viewExpandedText.orElse(null));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String databaseName;
        private String tableName;
        private TableType tableType;
        private Storage storage;
        private List<Column> dataColumns = List.of();
        private List<Column> partitionColumns = List.of();
        private Map<String, String> parameters = Map.of();
        private String viewOriginalText;
        private String viewExpandedText;

        public Builder setDatabaseName(String databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder setTableType(TableType tableType)
        {
            this.tableType = tableType;
            return this;
        }

        public Builder setStorage(Storage storage)
        {
            this.storage = storage;
            return this;
        }

        public Builder setDataColumns(List<Column> dataColumns)
        {
            this.dataColumns = dataColumns;
            return this;
        }

        public Builder setPartitionColumns(List<Column> partitionColumns)
        {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters)
        {
            this.parameters = parameters;
            return this;
        }

        public Builder setViewOriginalText(String viewOriginalText)
        {
            this.viewOriginalText = viewOriginalText;
            return this;
        }

        public Builder setViewExpandedText(String viewExpandedText)
        {
            this.viewExpandedText = viewExpandedText;
            return this;
        }

        public Table build()
        {
            return new Table(
                    databaseName,
                    tableName,
                    tableType,
                    storage,
                    dataColumns,
                    partitionColumns,
                    parameters,
                    Optional.ofNullable(viewOriginalText),
                    Optional.ofNullable(viewExpandedText));
        }
    }
}
