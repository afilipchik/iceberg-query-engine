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

import io.branching.metastore.server.config.MetastoreConfig;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Main server bootstrap for the branching metastore.
 * Simplified with Jersey Grizzly HTTP server.
 */
public class BranchingMetastoreServer
{
    private static final Logger log = LoggerFactory.getLogger(BranchingMetastoreServer.class);

    private BranchingMetastoreServer() {}

    public static void main(String[] args)
            throws IOException, InterruptedException
    {
        // Load configuration from environment or defaults
        MetastoreConfig config = loadConfig();

        // Create Jersey resource config
        ResourceConfig resourceConfig = new BranchingMetastoreModule(config);

        // Create and start HTTP server
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(
                URI.create("http://0.0.0.0:8080"),
                resourceConfig);

        log.info("======== BRANCHING METASTORE SERVER STARTED ========");
        log.info("Server running on http://0.0.0.0:8080");
        log.info("Press CTRL+C to stop...");

        // Keep server running
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down server...");
            server.shutdownNow();
        }));

        Thread.currentThread().join();
    }

    private static MetastoreConfig loadConfig()
    {
        MetastoreConfig config = new MetastoreConfig();

        // Allow override via environment variables
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
