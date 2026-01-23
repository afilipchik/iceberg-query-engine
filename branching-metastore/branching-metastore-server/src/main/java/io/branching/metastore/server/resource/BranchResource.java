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
import io.branching.metastore.common.exceptions.BranchAlreadyExistsException;
import io.branching.metastore.common.exceptions.BranchNotFoundException;
import io.branching.metastore.common.exceptions.MergeConflictException;
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

/**
 * REST resource for branch operations.
 * Git-like branch management: create, list, delete, merge.
 */
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
        catch (RuntimeException e) {
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
        catch (RuntimeException e) {
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

    /**
     * Request object for creating a branch.
     */
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
