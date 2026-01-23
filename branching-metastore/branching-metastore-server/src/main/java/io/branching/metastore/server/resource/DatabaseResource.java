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

/**
 * REST resource for database operations within a branch.
 * All database operations are branch-scoped.
 */
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
