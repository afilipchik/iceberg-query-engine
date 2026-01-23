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

/**
 * REST resource for table operations within a branch and database.
 * All table operations are branch-scoped.
 */
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

    /**
     * Request object for renaming a table.
     */
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
