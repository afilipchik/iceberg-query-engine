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
 * Database metadata, scoped to a specific branch.
 * Similar to Iceberg's Namespace concept but branch-isolated.
 */
@Immutable
public class Database
{
    private final String databaseName;
    private final Optional<String> location;
    private final Optional<String> comment;
    private final Map<String, String> properties;

    @JsonCreator
    public Database(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("location") Optional<String> location,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.databaseName = databaseName;
        this.location = location;
        this.comment = comment;
        this.properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    @JsonProperty
    public String databaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public Optional<String> location()
    {
        return location;
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
                .setDatabaseName(databaseName)
                .setLocation(location.orElse(null))
                .setComment(comment.orElse(null))
                .setProperties(properties);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String databaseName;
        private String location;
        private String comment;
        private Map<String, String> properties = Map.of();

        public Builder setDatabaseName(String databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder setLocation(String location)
        {
            this.location = location;
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

        public Database build()
        {
            return new Database(
                    databaseName,
                    Optional.ofNullable(location),
                    Optional.ofNullable(comment),
                    properties);
        }
    }
}
