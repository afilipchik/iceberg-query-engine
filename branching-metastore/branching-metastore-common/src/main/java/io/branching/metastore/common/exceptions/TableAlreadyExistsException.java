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
package io.branching.metastore.common.exceptions;

public class TableAlreadyExistsException extends MetastoreException
{
    private final String branchId;
    private final String databaseName;
    private final String tableName;

    public TableAlreadyExistsException(String message)
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

    public String branchId()
    {
        return branchId;
    }

    public String databaseName()
    {
        return databaseName;
    }

    public String tableName()
    {
        return tableName;
    }
}
