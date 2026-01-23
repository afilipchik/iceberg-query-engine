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
 * Storage information for a table (location, format, serialization).
 * Stored as JSONB following Stargate/Iceberg REST patterns (2025-2026).
 */
@Immutable
public class Storage
{
    private final Optional<String> location;
    private final Optional<String> inputFormat;
    private final Optional<String> outputFormat;
    private final Optional<String> serde;
    private final Map<String, String> serdeProperties;

    @JsonCreator
    public Storage(
            @JsonProperty("location") Optional<String> location,
            @JsonProperty("inputFormat") Optional<String> inputFormat,
            @JsonProperty("outputFormat") Optional<String> outputFormat,
            @JsonProperty("serde") Optional<String> serde,
            @JsonProperty("serdeProperties") Map<String, String> serdeProperties)
    {
        this.location = location;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.serde = serde;
        this.serdeProperties = serdeProperties == null ? Map.of() : Map.copyOf(serdeProperties);
    }

    @JsonProperty
    public Optional<String> location()
    {
        return location;
    }

    @JsonProperty
    public Optional<String> inputFormat()
    {
        return inputFormat;
    }

    @JsonProperty
    public Optional<String> outputFormat()
    {
        return outputFormat;
    }

    @JsonProperty
    public Optional<String> serde()
    {
        return serde;
    }

    @JsonProperty
    public Map<String, String> serdeProperties()
    {
        return serdeProperties;
    }

    public Builder toBuilder()
    {
        return new Builder()
                .setLocation(location.orElse(null))
                .setInputFormat(inputFormat.orElse(null))
                .setOutputFormat(outputFormat.orElse(null))
                .setSerde(serde.orElse(null))
                .setSerdeProperties(serdeProperties);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String location;
        private String inputFormat;
        private String outputFormat;
        private String serde;
        private Map<String, String> serdeProperties = Map.of();

        public Builder setLocation(String location)
        {
            this.location = location;
            return this;
        }

        public Builder setInputFormat(String inputFormat)
        {
            this.inputFormat = inputFormat;
            return this;
        }

        public Builder setOutputFormat(String outputFormat)
        {
            this.outputFormat = outputFormat;
            return this;
        }

        public Builder setSerde(String serde)
        {
            this.serde = serde;
            return this;
        }

        public Builder setSerdeProperties(Map<String, String> serdeProperties)
        {
            this.serdeProperties = serdeProperties;
            return this;
        }

        public Storage build()
        {
            return new Storage(
                    Optional.ofNullable(location),
                    Optional.ofNullable(inputFormat),
                    Optional.ofNullable(outputFormat),
                    Optional.ofNullable(serde),
                    serdeProperties);
        }
    }
}
