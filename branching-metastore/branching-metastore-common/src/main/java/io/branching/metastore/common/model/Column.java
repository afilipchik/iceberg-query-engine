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

import java.util.Optional;

/**
 * Column metadata for table schemas.
 * Immutable following Iceberg/Stargate patterns (2025-2026).
 */
@Immutable
public class Column
{
    private final String name;
    private final String type;
    private final Optional<String> comment;

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.name = name;
        this.type = type;
        this.comment = comment;
    }

    @JsonProperty
    public String name()
    {
        return name;
    }

    @JsonProperty
    public String type()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> comment()
    {
        return comment;
    }

    public Builder toBuilder()
    {
        return new Builder()
                .setName(name)
                .setType(type)
                .setComment(comment.orElse(null));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String name;
        private String type;
        private String comment;

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder setType(String type)
        {
            this.type = type;
            return this;
        }

        public Builder setComment(String comment)
        {
            this.comment = comment;
            return this;
        }

        public Column build()
        {
            return new Column(
                    name,
                    type,
                    Optional.ofNullable(comment));
        }
    }
}
