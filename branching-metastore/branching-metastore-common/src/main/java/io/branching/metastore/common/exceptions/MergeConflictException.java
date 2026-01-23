/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.branching.metastore.common.exceptions;

public class MergeConflictException extends MetastoreException
{
    private final String sourceBranch;
    private final String targetBranch;

    public MergeConflictException(String message)
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

    public String sourceBranch()
    {
        return sourceBranch;
    }

    public String targetBranch()
    {
        return targetBranch;
    }
}
