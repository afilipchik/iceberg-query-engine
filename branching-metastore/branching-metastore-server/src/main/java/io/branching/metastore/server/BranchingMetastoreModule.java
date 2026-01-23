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

/**
 * Guice module for the branching metastore server.
 * Simplified without Airlift dependencies.
 */
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

        // Register JAX-RS resources
        register(new BranchResource(new BranchingMetastoreImpl(jdbi)));
        register(new DatabaseResource(new BranchingMetastoreImpl(jdbi)));
        register(new TableResource(new BranchingMetastoreImpl(jdbi)));

        // Register JSON provider
        packages("io.branching.metastore.server.resource");
    }
}
