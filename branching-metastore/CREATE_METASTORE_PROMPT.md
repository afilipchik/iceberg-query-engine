# Branching Metastore Implementation Prompt

**Purpose**: This document contains a complete, detailed specification for implementing a catalog-level branching metastore for Trino/Iceberg from scratch. Follow these instructions exactly to replicate the implementation.

**Context**: You are implementing a cleanroom, production-grade branching metastore inspired by Nessie (2025-2026) with Git-like branching semantics. This uses BranchId as the primary isolation primitive for version control.

---

## TABLE OF CONTENTS
1. Architecture Overview
2. Technology Stack & Versions
3. Project Structure
4. Implementation Order (Follow This Exactly)
5. Detailed File Specifications
6. Common Patterns & Gotchas
7. Production Considerations
8. Testing & Verification

---

## 1. ARCHITECTURE OVERVIEW

### Core Design Principles

1. **BranchId as Primary Isolation**: All operations are branch-scoped. BranchId provides version isolation for metadata.

2. **Git-like Branching Semantics**: Inspired by Nessie (2025-2026) - branches can be created, merged, and deleted like Git branches.

3. **REST API (JAX-RS)**: Language-agnostic REST API with JSON content negotiation.

4. **JSONB for Complex Types**: PostgreSQL JSONB for schema/statistics storage. **IMPORTANT**: JSONB performance degrades for values >2KB (TOAST storage). Keep schema metadata under 2KB or use separate table for statistics.

5. **Immutable Data Models**: All model classes are immutable with builder pattern (thread-safe, following modern Java patterns).

### Key Architectural Decisions (Research-Backed)

| Decision | Rationale (2025-2026 Sources) |
|----------|-------------------------------|
| REST over Thrift | REST Catalog is recommended for new deployments (Conduktor 2025) |
| JSONB for schemas | PostgreSQL JSONB performs excellently for metadata storage |
| Flyway for migrations | Industry standard for database versioning |
| JDBI3 SQL Object | Clean database abstraction with compile-time SQL validation |

---

## 2. TECHNOLOGY STACK & VERSIONS

### Exact Versions (Use These - DO NOT Change)

```xml
<properties>
    <maven.compiler.source>21</maven.compiler.source>
    <maven.compiler.target>21</maven.compiler.target>

    <dep.airlift.version>254</dep.airlift.version>
    <dep.jackson.version>2.18.2</dep.jackson.version>
    <dep.jdbi.version>3.47.0</dep.jdbi.version>
    <dep.postgresql.version>42.7.4</dep.postgresql.version>
    <dep.flyway.version>11.1.0</dep.flyway.version>
    <dep.jersey.version>3.1.3</dep.jersey.version>
    <dep.guice.version>7.0.0</dep.guice.version>
    <dep.jetty.version>12.0.16</dep.jetty.version>
</properties>
```

### Critical Dependency Notes

1. **DO NOT use `io.airlift:units`** - it doesn't exist as a separate artifact
2. **DO NOT use `jersey-server` directly** - use `jersey-container-grizzly2-http` instead
3. **For JDBI JSON**: Use `JsonPlugin` only - `Jackson2Plugin` caused issues
4. **Airlift BOM**: Use `<type>pom</type><scope>import</scope>` for dependency management

### Required Dependencies

```xml
<!-- For common module -->
<dependency>
    <groupId>com.google.errorprone</groupId>
    <artifactId>error_prone_annotations</artifactId>
    <version>2.23.0</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-annotations</artifactId>
</dependency>

<!-- For server module -->
<dependency>
    <groupId>org.jdbi</groupId>
    <artifactId>jdbi3-core</artifactId>
</dependency>
<dependency>
    <groupId>org.jdbi</groupId>
    <artifactId>jdbi3-sqlobject</artifactId>
</dependency>
<dependency>
    <groupId>org.jdbi</groupId>
    <artifactId>jdbi3-json</artifactId>
</dependency>
<dependency>
    <groupId>org.jdbi</groupId>
    <artifactId>jdbi3-postgres</artifactId>
    <version>${dep.jdbi.version}</version> <!-- MUST specify version -->
</dependency>
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
</dependency>
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-core</artifactId>
</dependency>
<dependency>
    <groupId>org.flywaydb</groupId>
    <artifactId>flyway-database-postgresql</artifactId>
</dependency>
<dependency>
    <groupId>org.glassfish.jersey.containers</groupId>
    <artifactId>jersey-container-grizzly2-http</artifactId>
    <version>3.1.3</version>
</dependency>
<dependency>
    <groupId>org.glassfish.jersey.media</groupId>
    <artifactId>jersey-media-json-jackson</artifactId>
    <version>3.1.3</version>
</dependency>
<dependency>
    <groupId>org.glassfish.jersey.inject</groupId>
    <artifactId>jersey-hk2</artifactId>
    <version>3.1.3</version>
</dependency>
<dependency>
    <groupId>jakarta.ws.rs</groupId>
    <artifactId>jakarta.ws.rs-api</artifactId>
    <version>3.1.0</version>
</dependency>
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>2.0.9</version>
</dependency>
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>2.0.9</version>
</dependency>
```

---

## 3. PROJECT STRUCTURE

```
branching-metastore/
├── pom.xml                                    # Parent POM
├── branching-metastore-common/
│   └── pom.xml
│   └── src/main/java/io/branching/metastore/common/
│       ├── BranchingMetastore.java           # Main interface
│       ├── model/
│       │   ├── Branch.java
│       │   ├── BranchType.java
│       │   ├── Database.java
│       │   ├── Table.java
│       │   ├── TableType.java
│       │   ├── Column.java
│       │   └── Storage.java
│       └── exceptions/
│           ├── MetastoreException.java
│           ├── BranchNotFoundException.java
│           ├── BranchAlreadyExistsException.java
│           ├── TableNotFoundException.java
│           ├── TableAlreadyExistsException.java
│           └── MergeConflictException.java
├── branching-metastore-server/
│   └── pom.xml
│   └── src/main/java/io/branching/metastore/server/
│       ├── BranchingMetastoreServer.java      # Main entry point
│       ├── BranchingMetastoreModule.java      # Jersey config
│       ├── BranchingMetastoreImpl.java        # Service implementation
│       ├── config/
│       │   └── MetastoreConfig.java
│       ├── dao/
│       │   ├── BranchDao.java
│       │   ├── DatabaseDao.java
│       │   └── TableDao.java
│       └── resource/
│           ├── BranchResource.java
│           ├── DatabaseResource.java
│           └── TableResource.java
│   └── src/main/resources/db/migration/
│       └── V1__initial_schema.sql
└── branching-metastore-client/
    └── pom.xml                                # Placeholder for HTTP client
```

---

## 4. IMPLEMENTATION ORDER (FOLLOW THIS EXACTLY)

### Phase 1: Parent POM Setup

**File**: `pom.xml` (root)

**CRITICAL**: Use `<type>pom</type><scope>import</scope>` for Airlift BOM

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>io.branching</groupId>
    <artifactId>branching-metastore</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>pom</packaging>

    <name>Branching Metastore</name>
    <description>Catalog-level branching metastore for Trino/Iceberg</description>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>

        <dep.airlift.version>254</dep.airlift.version>
        <dep.jackson.version>2.18.2</dep.jackson.version>
        <dep.jdbi.version>3.47.0</dep.jdbi.version>
        <dep.postgresql.version>42.7.4</dep.postgresql.version>
        <dep.flyway.version>11.1.0</dep.flyway.version>
        <dep.jersey.version>3.1.3</dep.jersey.version>
        <dep.guice.version>7.0.0</dep.guice.version>
        <dep.jetty.version>12.0.16</dep.jetty.version>
    </properties>

    <modules>
        <module>branching-metastore-server</module>
        <module>branching-metastore-client</module>
        <module>branching-metastore-common</module>
    </modules>

    <dependencyManagement>
        <dependencies>
            <!-- Airlift BOM - CRITICAL: use import scope -->
            <dependency>
                <groupId>io.airlift</groupId>
                <artifactId>bom</artifactId>
                <version>${dep.airlift.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>

            <!-- Airlift units NOT in BOM - must specify explicitly -->
            <dependency>
                <groupId>io.airlift</groupId>
                <artifactId>units</artifactId>
                <version>${dep.airlift.version}</version>
            </dependency>

            <!-- Jackson -->
            <dependency>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-databind</artifactId>
                <version>${dep.jackson.version}</version>
            </dependency>
            <dependency>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-annotations</artifactId>
                <version>${dep.jackson.version}</version>
            </dependency>

            <!-- JDBI -->
            <dependency>
                <groupId>org.jdbi</groupId>
                <artifactId>jdbi3-core</artifactId>
                <version>${dep.jdbi.version}</version>
            </dependency>
            <dependency>
                <groupId>org.jdbi</groupId>
                <artifactId>jdbi3-sqlobject</artifactId>
                <version>${dep.jdbi.version}</version>
            </dependency>
            <dependency>
                <groupId>org.jdbi</groupId>
                <artifactId>jdbi3-json</artifactId>
                <version>${dep.jdbi.version}</version>
            </dependency>

            <!-- PostgreSQL -->
            <dependency>
                <groupId>org.postgresql</groupId>
                <artifactId>postgresql</artifactId>
                <version>${dep.postgresql.version}</version>
            </dependency>

            <!-- Flyway -->
            <dependency>
                <groupId>org.flywaydb</groupId>
                <artifactId>flyway-core</artifactId>
                <version>${dep.flyway.version}</version>
            </dependency>
            <dependency>
                <groupId>org.flywaydb</groupId>
                <artifactId>flyway-database-postgresql</artifactId>
                <version>${dep.flyway.version}</version>
            </dependency>

            <!-- Guice -->
            <dependency>
                <groupId>com.google.inject</groupId>
                <artifactId>guice</artifactId>
                <version>${dep.guice.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
</project>
```

### Phase 2: Common Module - Models

**File**: `branching-metastore-common/pom.xml`

**CRITICAL**: Must include Error Prone annotations for `@Immutable`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>io.branching</groupId>
        <artifactId>branching-metastore</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>branching-metastore-common</artifactId>
    <packaging>jar</packaging>

    <name>Branching Metastore Common</name>
    <description>Shared data models and exceptions</description>

    <dependencies>
        <!-- Jackson -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-annotations</artifactId>
        </dependency>

        <!-- Error Prone annotations - REQUIRED for @Immutable -->
        <dependency>
            <groupId>com.google.errorprone</groupId>
            <artifactId>error_prone_annotations</artifactId>
            <version>2.23.0</version>
        </dependency>

        <!-- Validation -->
        <dependency>
            <groupId>jakarta.validation</groupId>
            <artifactId>jakarta.validation-api</artifactId>
            <version>3.0.2</version>
        </dependency>
    </dependencies>
</project>
```

### Phase 3: Model Classes - Follow This Pattern EXACTLY

**CRITICAL PATTERN**: All models must be:
1. `@Immutable` from `com.google.errorprone.annotations`
2. Use `@JsonCreator` and `@JsonProperty` from Jackson
3. Have a Builder inner class
4. Use `Optional<String>` for nullable string fields
5. Builder methods take `String`, NOT `Optional<String>` (convert with `.orElse(null)`)

#### Branch.java

```java
package io.branching.metastore.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Map;
import java.util.Optional;

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
    public String branchId() { return branchId; }

    @JsonProperty
    public Optional<String> parentId() { return parentId; }

    @JsonProperty
    public Optional<String> headSnapshotId() { return headSnapshotId; }

    @JsonProperty
    public BranchType branchType() { return branchType; }

    @JsonProperty
    public Optional<String> comment() { return comment; }

    @JsonProperty
    public Map<String, String> properties() { return properties; }

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

        public Builder setParentId(String parentId)  // NOTE: String, not Optional
        {
            this.parentId = parentId;
            return this;
        }

        public Builder setHeadSnapshotId(String headSnapshotId)  // NOTE: String, not Optional
        {
            this.headSnapshotId = headSnapshotId;
            return this;
        }

        public Builder setBranchType(BranchType branchType)
        {
            this.branchType = branchType;
            return this;
        }

        public Builder setComment(String comment)  // NOTE: String, not Optional
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
```

#### BranchType.java

```java
package io.branching.metastore.common.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum BranchType
{
    MAIN("main"),
    FEATURE("feature"),
    RELEASE("release"),
    SNAPSHOT("snapshot");

    private final String value;

    BranchType(String value)
    {
        this.value = value;
    }

    @JsonValue
    public String getValue()
    {
        return value;
    }
}
```

#### Database.java

```java
package io.branching.metastore.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Map;
import java.util.Optional;

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
    public String databaseName() { return databaseName; }

    @JsonProperty
    public Optional<String> location() { return location; }

    @JsonProperty
    public Optional<String> comment() { return comment; }

    @JsonProperty
    public Map<String, String> properties() { return properties; }

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
```

#### TableType.java

```java
package io.branching.metastore.common.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum TableType
{
    MANAGED("MANAGED"),
    EXTERNAL("EXTERNAL"),
    VIEW("VIEW");

    private final String value;

    TableType(String value)
    {
        this.value = value;
    }

    @JsonValue
    public String getValue()
    {
        return value;
    }
}
```

#### Storage.java

```java
package io.branching.metastore.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Map;
import java.util.Optional;

@Immutable
public class Storage
{
    private final String storageFormat;
    private final String location;
    private final Optional<String> serdeInfo;
    private final Map<String, String> parameters;

    @JsonCreator
    public Storage(
            @JsonProperty("storageFormat") String storageFormat,
            @JsonProperty("location") String location,
            @JsonProperty("serdeInfo") Optional<String> serdeInfo,
            @JsonProperty("parameters") Map<String, String> parameters)
    {
        this.storageFormat = storageFormat;
        this.location = location;
        this.serdeInfo = serdeInfo;
        this.parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
    }

    @JsonProperty
    public String storageFormat() { return storageFormat; }

    @JsonProperty
    public String location() { return location; }

    @JsonProperty
    public Optional<String> serdeInfo() { return serdeInfo; }

    @JsonProperty
    public Map<String, String> parameters() { return parameters; }

    public Builder toBuilder()
    {
        return new Builder()
                .setStorageFormat(storageFormat)
                .setLocation(location)
                .setSerdeInfo(serdeInfo.orElse(null))
                .setParameters(parameters);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String storageFormat;
        private String location;
        private String serdeInfo;
        private Map<String, String> properties = Map.of();

        public Builder setStorageFormat(String storageFormat)
        {
            this.storageFormat = storageFormat;
            return this;
        }

        public Builder setLocation(String location)
        {
            this.location = location;
            return this;
        }

        public Builder setSerdeInfo(String serdeInfo)
        {
            this.serdeInfo = serdeInfo;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters)
        {
            this.properties = parameters == null ? Map.of() : parameters;
            return this;
        }

        public Storage build()
        {
            return new Storage(
                    storageFormat,
                    location,
                    Optional.ofNullable(serdeInfo),
                    properties);
        }
    }
}
```

#### Column.java

```java
package io.branching.metastore.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Optional;

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
    public String name() { return name; }

    @JsonProperty
    public String type() { return type; }

    @JsonProperty
    public Optional<String> comment() { return comment; }

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
```

#### Table.java

```java
package io.branching.metastore.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    public String databaseName() { return databaseName; }

    @JsonProperty
    public String tableName() { return tableName; }

    @JsonProperty
    public TableType tableType() { return tableType; }

    @JsonProperty
    public Storage storage() { return storage; }

    @JsonProperty
    public List<Column> dataColumns() { return dataColumns; }

    @JsonProperty
    public List<Column> partitionColumns() { return partitionColumns; }

    @JsonProperty
    public Map<String, String> parameters() { return parameters; }

    @JsonProperty
    public Optional<String> viewOriginalText() { return viewOriginalText; }

    @JsonProperty
    public Optional<String> viewExpandedText() { return viewExpandedText; }

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
        private Map<String, String> parameters = Map.of();  // NOTE: was "properties", changed to "parameters"
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
            this.dataColumns = dataColumns == null ? List.of() : dataColumns;
            return this;
        }

        public Builder setPartitionColumns(List<Column> partitionColumns)
        {
            this.partitionColumns = partitionColumns == null ? List.of() : partitionColumns;
            return this;
        }

        public Builder setParameters(Map<String, String> parameters)  // NOTE: "parameters" not "properties"
        {
            this.parameters = parameters == null ? Map.of() : parameters;
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
```

### Phase 4: Exception Classes

**CRITICAL PATTERN**: All exceptions must have TWO constructors:
1. Convenience constructor: `public ExceptionName(String message)`
2. Detailed constructor with fields

#### MetastoreException.java (Base)

```java
package io.branching.metastore.common.exceptions;

public class MetastoreException extends RuntimeException
{
    public MetastoreException(String message)
    {
        super(message);
    }

    public MetastoreException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
```

#### BranchNotFoundException.java

```java
package io.branching.metastore.common.exceptions;

public class BranchNotFoundException extends MetastoreException
{
    private final String branchId;

    public BranchNotFoundException(String message)  // Convenience constructor
    {
        super(message);
        this.branchId = null;
    }

    public BranchNotFoundException(String branchId)
    {
        super("Branch not found: " + branchId);
        this.branchId = branchId;
    }

    public String branchId() { return branchId; }
}
```

#### BranchAlreadyExistsException.java

```java
package io.branching.metastore.common.exceptions;

public class BranchAlreadyExistsException extends MetastoreException
{
    private final String branchId;

    public BranchAlreadyExistsException(String message)  // Convenience constructor
    {
        super(message);
        this.branchId = null;
    }

    public BranchAlreadyExistsException(String branchId)
    {
        super("Branch already exists: " + branchId);
        this.branchId = branchId;
    }

    public String branchId() { return branchId; }
}
```

#### TableNotFoundException.java

```java
package io.branching.metastore.common.exceptions;

public class TableNotFoundException extends MetastoreException
{
    private final String branchId;
    private final String databaseName;
    private final String tableName;

    public TableNotFoundException(String message)  // Convenience constructor
    {
        super(message);
        this.branchId = null;
        this.databaseName = null;
        this.tableName = null;
    }

    public TableNotFoundException(String branchId, String databaseName, String tableName)
    {
        super(String.format("Table not found: %s.%s in branch %s", databaseName, tableName, branchId));
        this.branchId = branchId;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public String branchId() { return branchId; }
    public String databaseName() { return databaseName; }
    public String tableName() { return tableName; }
}
```

#### TableAlreadyExistsException.java

```java
package io.branching.metastore.common.exceptions;

public class TableAlreadyExistsException extends MetastoreException
{
    private final String branchId;
    private final String databaseName;
    private final String tableName;

    public TableAlreadyExistsException(String message)  // Convenience constructor
    {
        super(message);
        this.branchId = null;
        this.databaseName = null;
        this.tableName = null;
    }

    public TableAlreadyExistsException(String branchId, String databaseName, String tableName)
    {
        super(String.format("Table already exists: %s.%s in branch %s", databaseName, tableName, branchId));
        this.branchId = branchId;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public String branchId() { return branchId; }
    public String databaseName() { return databaseName; }
    public String tableName() { return tableName; }
}
```

#### MergeConflictException.java

```java
package io.branching.metastore.common.exceptions;

public class MergeConflictException extends MetastoreException
{
    private final String sourceBranch;
    private final String targetBranch;

    public MergeConflictException(String message)  // Convenience constructor
    {
        super(message);
        this.sourceBranch = null;
        this.targetBranch = null;
    }

    public MergeConflictException(String sourceBranch, String targetBranch, String reason)
    {
        super(String.format("Merge conflict: cannot merge %s into %s. Reason: %s", sourceBranch, targetBranch, reason));
        this.sourceBranch = sourceBranch;
        this.targetBranch = targetBranch;
    }

    public String sourceBranch() { return sourceBranch; }
    public String targetBranch() { return targetBranch; }
}
```

### Phase 5: Main Interface

**File**: `BranchingMetastore.java`

**CRITICAL**: TableInfo is a nested record inside the interface, not a separate class.

```java
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
 * Provides Git-like operations on metadata with branch-scoped isolation.
 */
public interface BranchingMetastore
{
    // ========== Branch Operations ==========

    Collection<Branch> getAllBranches();

    Optional<Branch> getBranch(String branchId) throws BranchNotFoundException;

    void createBranch(String branchId, Optional<String> parentId, BranchType branchType)
            throws BranchAlreadyExistsException;

    void deleteBranch(String branchId) throws BranchNotFoundException;

    void mergeBranch(String sourceBranch, String targetBranch, String commitMessage)
            throws MergeConflictException;

    // ========== Database Operations ==========

    Collection<String> listDatabases(String branchId);

    Optional<Database> getDatabase(String branchId, String databaseName);

    void createDatabase(String branchId, Database database) throws BranchNotFoundException;

    void dropDatabase(String branchId, String databaseName) throws BranchNotFoundException;

    void renameDatabase(String branchId, String databaseName, String newDatabaseName)
            throws BranchNotFoundException;

    // ========== Table Operations ==========

    Collection<TableInfo> listTables(String branchId, String databaseName);

    Optional<Table> getTable(String branchId, String databaseName, String tableName)
            throws TableNotFoundException;

    void createTable(String branchId, Table table) throws TableAlreadyExistsException;

    void dropTable(String branchId, String databaseName, String tableName)
            throws TableNotFoundException;

    void replaceTable(String branchId, Table table) throws TableNotFoundException;

    void renameTable(String branchId, String databaseName, String tableName,
                     String newDatabaseName, String newTableName)
            throws TableNotFoundException;

    void addColumn(String branchId, String databaseName, String tableName, Column column)
            throws TableNotFoundException;

    void renameColumn(String branchId, String databaseName, String tableName,
                      String columnName, String newColumnName)
            throws TableNotFoundException;

    void dropColumn(String branchId, String databaseName, String tableName, String columnName)
            throws TableNotFoundException;

    GetTablesResult getTables(String branchId, String databaseName,
                             Optional<Integer> limit, Optional<String> nextToken);

    /**
     * Table info for listing operations.
     * NOTE: This is a NESTED record, not a separate class!
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
```

### Phase 6: Server Module - DAO Layer

**File**: `branching-metastore-server/pom.xml`

**CRITICAL**: Do NOT use Airlift dependencies. Use simplified Jersey Grizzly setup.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>io.branching</groupId>
        <artifactId>branching-metastore</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>branching-metastore-server</artifactId>
    <packaging>jar</packaging>

    <name>Branching Metastore Server</name>
    <description>REST metastore server with catalog-level branching</description>

    <dependencies>
        <!-- Common module -->
        <dependency>
            <groupId>io.branching</groupId>
            <artifactId>branching-metastore-common</artifactId>
            <version>${project.version}</version>
        </dependency>

        <!-- JDBI -->
        <dependency>
            <groupId>org.jdbi</groupId>
            <artifactId>jdbi3-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.jdbi</groupId>
            <artifactId>jdbi3-sqlobject</artifactId>
        </dependency>
        <dependency>
            <groupId>org.jdbi</groupId>
            <artifactId>jdbi3-json</artifactId>
        </dependency>
        <dependency>
            <groupId>org.jdbi</groupId>
            <artifactId>jdbi3-postgres</artifactId>
            <version>${dep.jdbi.version}</version>
        </dependency>

        <!-- PostgreSQL -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
        </dependency>

        <!-- Flyway -->
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>

        <!-- Jersey (JAX-RS) -->
        <dependency>
            <groupId>org.glassfish.jersey.containers</groupId>
            <artifactId>jersey-container-grizzly2-http</artifactId>
            <version>3.1.3</version>
        </dependency>
        <dependency>
            <groupId>org.glassfish.jersey.media</groupId>
            <artifactId>jersey-media-json-jackson</artifactId>
            <version>3.1.3</version>
        </dependency>
        <dependency>
            <groupId>org.glassfish.jersey.inject</groupId>
            <artifactId>jersey-hk2</artifactId>
            <version>3.1.3</version>
        </dependency>
        <dependency>
            <groupId>jakarta.ws.rs</groupId>
            <artifactId>jakarta.ws.rs-api</artifactId>
            <version>3.1.0</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>2.0.9</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>2.0.9</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.13.0</version>
                <configuration>
                    <source>21</source>
                    <target>21</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

#### BranchDao.java

```java
package io.branching.metastore.server.dao;

import io.branching.metastore.common.model.Branch;
import io.branching.metastore.common.model.BranchType;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

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

    @SqlUpdate("DELETE FROM branches WHERE branch_id = :branchId")
    boolean deleteBranch(String branchId);

    @SqlQuery("SELECT COUNT(*) > 0 FROM branches WHERE branch_id = :branchId")
    boolean branchExists(String branchId);

    @SqlUpdate("DELETE FROM branches WHERE parent_id = :branchId")
    int deleteChildBranches(String branchId);
}
```

#### DatabaseDao.java

```java
package io.branching.metastore.server.dao;

import io.branching.metastore.common.model.Database;
import org.jdbi.v3.json.Json;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Optional;

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
}
```

#### TableDao.java

```java
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
import java.util.Optional;

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
}
```

### Phase 7: Service Implementation

**File**: `BranchingMetastoreImpl.java`

**CRITICAL PATTERNS**:
1. Use `jdbi.withExtension()` for read operations (returns value)
2. Use `jdbi.useExtension()` for write operations (void)
3. Builder methods take `String`, NOT `Optional<String>` - convert with `.orElse(null)`
4. All exception handling uses `requireNonNull()` for null checks

```java
package io.branching.metastore.server;

import io.branching.metastore.common.BranchingMetastore;
import io.branching.metastore.common.exceptions.*;
import io.branching.metastore.common.model.*;
import io.branching.metastore.server.dao.BranchDao;
import io.branching.metastore.server.dao.DatabaseDao;
import io.branching.metastore.server.dao.TableDao;
import org.jdbi.v3.core.Jdbi;

import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BranchingMetastoreImpl
        implements BranchingMetastore
{
    private final Jdbi jdbi;

    public BranchingMetastoreImpl(Jdbi jdbi)
    {
        this.jdbi = requireNonNull(jdbi, "jdbi is null");
    }

    @Override
    public Collection<Branch> getAllBranches()
    {
        return jdbi.withExtension(BranchDao.class, dao -> dao.listAllBranches());
    }

    @Override
    public Optional<Branch> getBranch(String branchId) throws BranchNotFoundException
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

            // CRITICAL: Convert Optional to String with .orElse(null) for builder
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
    public void deleteBranch(String branchId) throws BranchNotFoundException
    {
        requireNonNull(branchId, "branchId is null");

        jdbi.useExtension(BranchDao.class, dao -> {
            if (!dao.branchExists(branchId)) {
                throw new BranchNotFoundException("Branch not found: " + branchId);
            }

            dao.deleteChildBranches(branchId);
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

            // Collect all databases from source branch
            List<Database> sourceDatabases = databaseDao.listDatabases(sourceBranch);

            // Copy databases and tables from source to target
            for (Database database : sourceDatabases) {
                Optional<Database> existingDb = databaseDao.getDatabase(targetBranch, database.databaseName());
                if (existingDb.isEmpty()) {
                    databaseDao.insertDatabase(targetBranch, database);
                }

                // Copy tables
                List<String> sourceTableNames = handle.select(
                        "SELECT table_name FROM tables WHERE branch_id = ? AND database_name = ?",
                        sourceBranch, database.databaseName())
                        .mapTo(String.class)
                        .list();

                for (String tableName : sourceTableNames) {
                    Table sourceTable = tableDao.getTable(sourceBranch, database.databaseName(), tableName)
                            .orElseThrow();

                    // Use updateTable for upsert
                    tableDao.updateTable(targetBranch, sourceTable);
                }
            }
        });
    }

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
    public void createDatabase(String branchId, Database database) throws BranchNotFoundException
    {
        requireNonNull(branchId, "branchId is null");
        requireNonNull(database, "database is null");

        jdbi.useExtension(DatabaseDao.class, dao -> {
            if (!jdbi.withExtension(BranchDao.class, bDao -> bDao.branchExists(branchId))) {
                throw new BranchNotFoundException("Branch not found: " + branchId);
            }

            if (dao.databaseExists(branchId, database.databaseName())) {
                return; // Already exists, idempotent
            }

            dao.insertDatabase(branchId, database);
        });
    }

    @Override
    public void dropDatabase(String branchId, String databaseName) throws BranchNotFoundException
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
    public void createTable(String branchId, Table table) throws TableAlreadyExistsException
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
    public void replaceTable(String branchId, Table table) throws TableNotFoundException
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
```

### Phase 8: REST API Resources

**CRITICAL PATTERNS**:
1. Use `catch (Exception e)` NOT `catch (SpecificException | RuntimeException e)` - this causes compilation errors
2. TableInfo is `BranchingMetastore.TableInfo`, NOT imported from model package
3. Use `@Inject` for constructor injection

#### BranchResource.java

```java
package io.branching.metastore.server.resource;

import io.branching.metastore.common.BranchingMetastore;
import io.branching.metastore.common.exceptions.*;
import io.branching.metastore.common.model.Branch;
import io.branching.metastore.common.model.BranchType;
import jakarta.inject.Inject;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.util.Collection;
import java.util.Optional;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static jakarta.ws.rs.core.Response.Status.CONFLICT;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/branch")
public class BranchResource
{
    private final BranchingMetastore metastore;

    @Inject
    public BranchResource(BranchingMetastore metastore)
    {
        this.metastore = metastore;
    }

    @GET
    @Produces(APPLICATION_JSON)
    public Collection<Branch> getAllBranches()
    {
        try {
            return metastore.getAllBranches();
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @GET
    @Path("{branchId}")
    @Produces(APPLICATION_JSON)
    public Response getBranch(@PathParam("branchId") String branchId)
    {
        try {
            Optional<Branch> branch = metastore.getBranch(branchId);
            if (branch.isEmpty()) {
                return Response.status(NOT_FOUND)
                        .entity("Branch not found: " + branchId)
                        .build();
            }
            return Response.ok(branch.get()).build();
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @POST
    @Consumes(APPLICATION_JSON)
    public Response createBranch(CreateBranchRequest request, @Context UriInfo uriInfo)
    {
        try {
            metastore.createBranch(request.branchId(), request.parentId(), request.branchType());
            return Response.created(uriInfo.getRequestUriBuilder()
                    .path(request.branchId())
                    .build())
                    .build();
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @DELETE
    @Path("{branchId}")
    public void deleteBranch(@PathParam("branchId") String branchId)
    {
        try {
            metastore.deleteBranch(branchId);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @POST
    @Path("{sourceBranch}/merge/{targetBranch}")
    @Consumes(TEXT_PLAIN)
    public void mergeBranch(
            @PathParam("sourceBranch") String sourceBranch,
            @PathParam("targetBranch") String targetBranch,
            String commitMessage)
    {
        try {
            metastore.mergeBranch(sourceBranch, targetBranch, commitMessage);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    private static RuntimeException handleException(Exception e)
    {
        if (e instanceof BranchNotFoundException) {
            throw new ClientErrorException(
                    e.getMessage(),
                    Response.status(NOT_FOUND).entity(e.getMessage()).build(),
                    e);
        }
        if (e instanceof BranchAlreadyExistsException || e instanceof MergeConflictException) {
            throw new ClientErrorException(
                    e.getMessage(),
                    Response.status(CONFLICT).entity(e.getMessage()).build(),
                    e);
        }
        throw new RuntimeException(e.getMessage(), e);
    }

    public record CreateBranchRequest(
            String branchId,
            Optional<String> parentId,
            BranchType branchType)
    {
        public CreateBranchRequest
        {
            if (branchId == null || branchId.isBlank()) {
                throw new IllegalArgumentException("branchId is required");
            }
        }
    }
}
```

#### DatabaseResource.java

```java
package io.branching.metastore.server.resource;

import io.branching.metastore.common.BranchingMetastore;
import io.branching.metastore.common.exceptions.BranchNotFoundException;
import io.branching.metastore.common.model.Database;
import jakarta.inject.Inject;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.util.Collection;
import java.util.Optional;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/branch/{branchId}/database")
public class DatabaseResource
{
    private final BranchingMetastore metastore;

    @Inject
    public DatabaseResource(BranchingMetastore metastore)
    {
        this.metastore = metastore;
    }

    @GET
    @Produces(APPLICATION_JSON)
    public Collection<String> listDatabases(@PathParam("branchId") String branchId)
    {
        try {
            return metastore.listDatabases(branchId);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @GET
    @Path("{databaseName}")
    @Produces(APPLICATION_JSON)
    public Response getDatabase(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName)
    {
        try {
            Optional<Database> database = metastore.getDatabase(branchId, databaseName);
            if (database.isEmpty()) {
                return Response.status(NOT_FOUND)
                        .entity("Database not found: " + databaseName)
                        .build();
            }
            return Response.ok(database.get()).build();
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @POST
    @Consumes(APPLICATION_JSON)
    public Response createDatabase(
            @PathParam("branchId") String branchId,
            Database database,
            @Context UriInfo uriInfo)
    {
        try {
            metastore.createDatabase(branchId, database);
            return Response.created(uriInfo.getRequestUriBuilder()
                    .path(database.databaseName())
                    .build())
                    .build();
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @DELETE
    @Path("{databaseName}")
    public void dropDatabase(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName)
    {
        try {
            metastore.dropDatabase(branchId, databaseName);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @POST
    @Path("{databaseName}:rename")
    @Consumes(TEXT_PLAIN)
    public void renameDatabase(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            String newDatabaseName)
    {
        try {
            metastore.renameDatabase(branchId, databaseName, newDatabaseName);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    private static RuntimeException handleException(Exception e)
    {
        if (e instanceof BranchNotFoundException) {
            throw new ClientErrorException(
                    e.getMessage(),
                    Response.status(NOT_FOUND).entity(e.getMessage()).build(),
                    e);
        }
        throw new RuntimeException(e.getMessage(), e);
    }
}
```

#### TableResource.java

**CRITICAL**: Use `BranchingMetastore.TableInfo`, NOT import from model package

```java
package io.branching.metastore.server.resource;

import io.branching.metastore.common.BranchingMetastore;
import io.branching.metastore.common.exceptions.BranchNotFoundException;
import io.branching.metastore.common.exceptions.TableAlreadyExistsException;
import io.branching.metastore.common.exceptions.TableNotFoundException;
import io.branching.metastore.common.model.Column;
import io.branching.metastore.common.model.Table;
import jakarta.inject.Inject;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.util.Collection;
import java.util.Optional;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.CONFLICT;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/branch/{branchId}/database/{databaseName}/table")
public class TableResource
{
    private final BranchingMetastore metastore;

    @Inject
    public TableResource(BranchingMetastore metastore)
    {
        this.metastore = metastore;
    }

    @GET
    @Produces(APPLICATION_JSON)
    public Collection<BranchingMetastore.TableInfo> listTables(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName)
    {
        try {
            return metastore.listTables(branchId, databaseName);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @GET
    @Path("tables")
    @Produces(APPLICATION_JSON)
    public Response getTables(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            @QueryParam("limit") Integer limit,
            @QueryParam("nextToken") String nextToken)
    {
        try {
            BranchingMetastore.GetTablesResult result = metastore.getTables(
                    branchId,
                    databaseName,
                    Optional.ofNullable(limit),
                    Optional.ofNullable(nextToken));
            return Response.ok(result).build();
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @GET
    @Path("{tableName}")
    @Produces(APPLICATION_JSON)
    public Response getTable(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            @PathParam("tableName") String tableName)
    {
        try {
            Optional<Table> table = metastore.getTable(branchId, databaseName, tableName);
            if (table.isEmpty()) {
                return Response.status(NOT_FOUND)
                        .entity("Table not found: " + databaseName + "." + tableName)
                        .build();
            }
            return Response.ok(table.get()).build();
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @POST
    @Consumes(APPLICATION_JSON)
    public Response createTable(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            Table table,
            @Context UriInfo uriInfo)
    {
        if (!databaseName.equals(table.databaseName())) {
            throw new ClientErrorException(
                    "Database name does not match name in table",
                    Response.status(BAD_REQUEST).entity("Database name mismatch").build());
        }
        try {
            metastore.createTable(branchId, table);
            return Response.created(uriInfo.getRequestUriBuilder()
                    .path(table.tableName())
                    .build())
                    .build();
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @DELETE
    @Path("{tableName}")
    public void dropTable(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            @PathParam("tableName") String tableName)
    {
        try {
            metastore.dropTable(branchId, databaseName, tableName);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @PUT
    @Path("{tableName}")
    @Consumes(APPLICATION_JSON)
    public void replaceTable(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            @PathParam("tableName") String tableName,
            Table table)
    {
        if (!databaseName.equals(table.databaseName()) || !tableName.equals(table.tableName())) {
            throw new ClientErrorException(
                    "Table name does not match name in table",
                    Response.status(BAD_REQUEST).entity("Table name mismatch").build());
        }
        try {
            metastore.replaceTable(branchId, table);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @POST
    @Path("{tableName}:rename")
    @Consumes(APPLICATION_JSON)
    public void renameTable(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            @PathParam("tableName") String tableName,
            RenameTableRequest request)
    {
        try {
            metastore.renameTable(branchId, databaseName, tableName,
                    request.newDatabaseName(), request.newTableName());
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @POST
    @Path("{tableName}/column")
    @Consumes(APPLICATION_JSON)
    public Response addColumn(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            @PathParam("tableName") String tableName,
            Column column,
            @Context UriInfo uriInfo)
    {
        try {
            metastore.addColumn(branchId, databaseName, tableName, column);
            return Response.created(uriInfo.getRequestUriBuilder()
                    .path(column.name())
                    .build())
                    .build();
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @POST
    @Path("{tableName}/column/{columnName}:rename")
    @Consumes(TEXT_PLAIN)
    public void renameColumn(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            @PathParam("tableName") String tableName,
            @PathParam("columnName") String columnName,
            String newColumnName)
    {
        try {
            metastore.renameColumn(branchId, databaseName, tableName,
                    columnName, newColumnName);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    @DELETE
    @Path("{tableName}/column/{columnName}")
    public void dropColumn(
            @PathParam("branchId") String branchId,
            @PathParam("databaseName") String databaseName,
            @PathParam("tableName") String tableName,
            @PathParam("columnName") String columnName)
    {
        try {
            metastore.dropColumn(branchId, databaseName, tableName, columnName);
        }
        catch (Exception e) {
            throw handleException(e);
        }
    }

    private static RuntimeException handleException(Exception e)
    {
        if (e instanceof TableNotFoundException) {
            throw new ClientErrorException(
                    e.getMessage(),
                    Response.status(NOT_FOUND).entity(e.getMessage()).build(),
                    e);
        }
        if (e instanceof TableAlreadyExistsException) {
            throw new ClientErrorException(
                    e.getMessage(),
                    Response.status(CONFLICT).entity(e.getMessage()).build(),
                    e);
        }
        if (e instanceof BranchNotFoundException) {
            throw new ClientErrorException(
                    e.getMessage(),
                    Response.status(NOT_FOUND).entity(e.getMessage()).build(),
                    e);
        }
        throw new RuntimeException(e.getMessage(), e);
    }

    public record RenameTableRequest(String newDatabaseName, String newTableName)
    {
        public RenameTableRequest
        {
            if (newDatabaseName == null || newDatabaseName.isBlank()) {
                throw new IllegalArgumentException("newDatabaseName is required");
            }
            if (newTableName == null || newTableName.isBlank()) {
                throw new IllegalArgumentException("newTableName is required");
            }
        }
    }
}
```

### Phase 9: Server Configuration & Bootstrap

#### MetastoreConfig.java

**CRITICAL**: Do NOT use Airlift `@Config` annotation - this is a simplified config class

```java
package io.branching.metastore.server.config;

/**
 * Configuration for the branching metastore server.
 */
public class MetastoreConfig
{
    private String dbUrl = "jdbc:postgresql://localhost:5432/branching_metastore";
    private String dbUser = "postgres";
    private String dbPassword;

    public MetastoreConfig setDbUrl(String dbUrl)
    {
        this.dbUrl = dbUrl;
        return this;
    }

    public String getDbUrl()
    {
        return dbUrl;
    }

    public MetastoreConfig setDbUser(String dbUser)
    {
        this.dbUser = dbUser;
        return this;
    }

    public String getDbUser()
    {
        return dbUser;
    }

    public MetastoreConfig setDbPassword(String dbPassword)
    {
        this.dbPassword = dbPassword;
        return this;
    }

    public String getDbPassword()
    {
        return dbPassword;
    }
}
```

#### BranchingMetastoreModule.java

**CRITICAL**: Use `JsonPlugin` only - do NOT use `Jackson2JsonPlugin`

```java
package io.branching.metastore.server;

import io.branching.metastore.common.BranchingMetastore;
import io.branching.metastore.server.config.MetastoreConfig;
import io.branching.metastore.server.resource.BranchResource;
import io.branching.metastore.server.resource.DatabaseResource;
import io.branching.metastore.server.resource.TableResource;
import org.flywaydb.core.Flyway;
import org.glassfish.jersey.server.ResourceConfig;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.json.JsonPlugin;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

public class BranchingMetastoreModule
        extends ResourceConfig
{
    public BranchingMetastoreModule(MetastoreConfig config)
    {
        // Initialize JDBI with Flyway migrations
        Flyway.configure()
                .dataSource(config.getDbUrl(), config.getDbUser(), config.getDbPassword())
                .load()
                .migrate();

        Jdbi jdbi = Jdbi.create(config.getDbUrl(), config.getDbUser(), config.getDbPassword())
                .installPlugin(new SqlObjectPlugin())
                .installPlugin(new PostgresPlugin())
                .installPlugin(new JsonPlugin());

        // Create service instance
        BranchingMetastore metastore = new BranchingMetastoreImpl(jdbi);

        // Register JAX-RS resources
        register(new BranchResource(metastore));
        register(new DatabaseResource(metastore));
        register(new TableResource(metastore));

        // Register JSON provider
        packages("io.branching.metastore.server.resource");
    }
}
```

#### BranchingMetastoreServer.java

**CRITICAL**: Must declare `throws InterruptedException`

```java
package io.branching.metastore.server;

import io.branching.metastore.server.config.MetastoreConfig;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class BranchingMetastoreServer
{
    private static final Logger log = LoggerFactory.getLogger(BranchingMetastoreServer.class);

    private BranchingMetastoreServer() {}

    public static void main(String[] args)
            throws IOException, InterruptedException  // CRITICAL: InterruptedException
    {
        MetastoreConfig config = loadConfig();

        ResourceConfig resourceConfig = new BranchingMetastoreModule(config);

        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(
                URI.create("http://0.0.0.0:8080"),
                resourceConfig);

        log.info("======== BRANCHING METASTORE SERVER STARTED ========");
        log.info("Server running on http://0.0.0.0:8080");
        log.info("Press CTRL+C to stop...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down server...");
            server.shutdownNow();
        }));

        Thread.currentThread().join();
    }

    private static MetastoreConfig loadConfig()
    {
        MetastoreConfig config = new MetastoreConfig();

        String dbUrl = System.getenv("METASTORE_DB_URL");
        if (dbUrl != null && !dbUrl.isBlank()) {
            config.setDbUrl(dbUrl);
        }

        String dbUser = System.getenv("METASTORE_DB_USER");
        if (dbUser != null && !dbUser.isBlank()) {
            config.setDbUser(dbUser);
        }

        String dbPassword = System.getenv("METASTORE_DB_PASSWORD");
        if (dbPassword != null && !dbPassword.isBlank()) {
            config.setDbPassword(dbPassword);
        }

        return config;
    }
}
```

### Phase 10: Database Migration Script

**File**: `src/main/resources/db/migration/V1__initial_schema.sql`

```sql
-- Licensed under the Apache License, Version 2.0
-- Branching Metastore Database Schema
-- PostgreSQL with JSONB for complex types

-- Branches table: core branching metadata
CREATE TABLE IF NOT EXISTS branches (
    branch_id VARCHAR(255) PRIMARY KEY,
    parent_id VARCHAR(255),
    head_snapshot_id VARCHAR(255),
    branch_type VARCHAR(50) NOT NULL,
    comment TEXT,
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_parent_branch FOREIGN KEY (parent_id) REFERENCES branches(branch_id) ON DELETE SET NULL
);

CREATE INDEX idx_branches_type ON branches(branch_type);

-- Databases table: branch-scoped database metadata
CREATE TABLE IF NOT EXISTS databases (
    id BIGSERIAL PRIMARY KEY,
    branch_id VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    location TEXT,
    comment TEXT,
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_databases_branch FOREIGN KEY (branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE,
    CONSTRAINT uq_databases_branch_name UNIQUE (branch_id, database_name)
);

CREATE INDEX idx_databases_branch ON databases(branch_id);

-- Tables table: branch-scoped table metadata with JSONB for schema
CREATE TABLE IF NOT EXISTS tables (
    id BIGSERIAL PRIMARY KEY,
    branch_id VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    table_type VARCHAR(50) NOT NULL,
    storage JSONB NOT NULL,
    data_columns JSONB NOT NULL,
    partition_columns JSONB DEFAULT '[]',
    parameters JSONB DEFAULT '{}',
    view_original_text TEXT,
    view_expanded_text TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_tables_branch FOREIGN KEY (branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE,
    CONSTRAINT fk_tables_database FOREIGN KEY (database_name, branch_id) REFERENCES databases(database_name, branch_id) ON DELETE CASCADE,
    CONSTRAINT uq_tables_branch_db_name UNIQUE (branch_id, database_name, table_name)
);

CREATE INDEX idx_tables_branch_db ON tables(branch_id, database_name);
CREATE INDEX idx_tables_branch_db_name ON tables(branch_id, database_name, table_name);

-- GIN indexes for JSONB query performance (CRITICAL for production)
CREATE INDEX idx_tables_storage_gin ON tables USING gin (storage);
CREATE INDEX idx_tables_data_columns_gin ON tables USING gin (data_columns);
CREATE INDEX idx_tables_parameters_gin ON tables USING gin (parameters);

-- Insert main branch
INSERT INTO branches (branch_id, parent_id, branch_type, comment, properties)
VALUES ('main', NULL, 'MAIN', 'Main branch', '{}')
ON CONFLICT (branch_id) DO NOTHING;
```

### Phase 11: Client Module Placeholder

**File**: `branching-metastore-client/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>io.branching</groupId>
        <artifactId>branching-metastore</artifactId>
        <version>1.0-SNAPSHOT</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>branching-metastore-client</artifactId>
    <packaging>jar</packaging>

    <name>Branching Metastore Client</name>
    <description>HTTP client for the branching metastore REST API</description>

    <dependencies>
        <dependency>
            <groupId>io.branching</groupId>
            <artifactId>branching-metastore-common</artifactId>
            <version>${project.version}</version>
        </dependency>

        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-annotations</artifactId>
        </dependency>

        <dependency>
            <groupId>jakarta.inject</groupId>
            <artifactId>jakarta.inject-api</artifactId>
            <version>2.0.1</version>
        </dependency>
    </dependencies>
</project>
```

---

## 6. COMMON PATTERNS & GOTCHAS

### Builder Pattern with Optional Fields

**WRONG**:
```java
Branch.builder()
    .setParentId(Optional.of("main"))  // WRONG! Builder takes String
```

**RIGHT**:
```java
Branch.builder()
    .setParentId(Optional.of("main").orElse(null))  // Convert Optional to String
```

### Multi-Catch Statements

**WRONG**:
```java
catch (BranchNotFoundException | RuntimeException e)  // WRONG! Subclass of RuntimeException
```

**RIGHT**:
```java
catch (Exception e)  // Catches everything
```

### Nested Records in Interfaces

**WRONG**:
```java
import io.branching.metastore.common.model.TableInfo;  // WRONG! TableInfo is nested
```

**RIGHT**:
```java
// No import needed
Collection<BranchingMetastore.TableInfo> tables;  // Use fully qualified name
```

### JDBI Plugin Setup

**WRONG**:
```java
.installPlugin(new JsonPlugin())
.installPlugin(new Jackson2JsonPlugin())  // WRONG! Class doesn't exist
```

**RIGHT**:
```java
.installPlugin(new SqlObjectPlugin())
.installPlugin(new PostgresPlugin())
.installPlugin(new JsonPlugin())  // Only these three
```

### Exception Constructors

All custom exceptions must have TWO constructors:
1. `public ExceptionName(String message)` - for convenience
2. `public ExceptionName(...fields)` - for detailed info

---

## 7. PRODUCTION CONSIDERATIONS

### Performance Optimization

**JSONB Indexing**: The database migration includes GIN indexes on JSONB columns. These are CRITICAL for query performance when filtering by schema or parameters.

**Connection Pooling**: For production deployments, add HikariCP connection pooling:
```xml
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>6.2.1</version>
</dependency>
```

### Security Notes

This implementation intentionally omits authentication/authorization for simplicity. For production use, add:
- TLS/HTTPS support
- Authentication (OAuth2, JWT, or mTLS)
- Authorization layer (role-based access control)

### Monitoring

Add metrics collection (Micrometer/Prometheus) for:
- Request latency by endpoint
- Database connection pool metrics
- Branch merge operation counts
- Failed transaction rate

---

## 8. TESTING & VERIFICATION

### Build Command

```bash
cd /path/to/branching-metastore
mvn clean compile -DskipTests
```

**Expected Output**:
```
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  0.9s
```

### Common Build Errors & Solutions

| Error | Solution |
|-------|----------|
| `cannot find symbol: class Immutable` | Add `error_prone_annotations` dependency to common module |
| `cannot find symbol: class TableInfo` | Use `BranchingMetastore.TableInfo` instead of importing from model package |
| `actual and formal argument lists differ in length` | Builder methods take `String`, not `Optional<String>` - use `.orElse(null)` |
| `Alternatives in a multi-catch statement cannot be related by subclassing` | Use `catch (Exception e)` instead of specific exception list |
| `Could not find artifact io.airlift:units` | Add explicit dependency with version in parent POM dependencyManagement |
| `class Jackson2JsonPlugin cannot be found` | Do NOT use this plugin - only use `JsonPlugin` |
| `unreported exception InterruptedException` | Add `throws InterruptedException` to main() method signature |

### Quick Verification Checklist

- [ ] Parent POM has `<type>pom</type><scope>import</scope>` for Airlift BOM
- [ ] All model classes use `@Immutable` from `com.google.errorprone.annotations`
- [ ] All model builders have `private String` fields (not `Optional<String>`)
- [ ] All exceptions have convenience constructor `public ExceptionName(String message)`
- [ ] JDBI uses `JsonPlugin` only (no Jackson2Plugin)
- [ ] Resources use `catch (Exception e)` not multi-catch with RuntimeException
- [ ] TableInfo is referenced as `BranchingMetastore.TableInfo`
- [ ] `jdbi3-postgres` has explicit version specified
- [ ] Main method declares `throws InterruptedException`

---

## END OF PROMPT

Follow this specification exactly to implement the branching metastore. The order of phases is intentional - each phase builds on the previous ones. Do not skip ahead without completing earlier phases first.
