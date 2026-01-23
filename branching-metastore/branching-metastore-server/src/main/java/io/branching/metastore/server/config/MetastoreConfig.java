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
