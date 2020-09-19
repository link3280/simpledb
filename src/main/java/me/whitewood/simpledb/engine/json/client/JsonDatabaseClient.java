/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.whitewood.simpledb.engine.json.client;

import com.fasterxml.jackson.databind.JsonNode;
import me.whitewood.simpledb.engine.json.common.JsonTable;

import java.io.IOException;
import java.util.List;

/**
 * JsonDatabaseClient is the client to interact with json databases.
 * Currently, supports reads only.
 **/
public interface JsonDatabaseClient {

    /**
     * List the names of the available {@link JsonTable} in the current database that matches an optional pattern.
     * @param pattern Optional pattern that the table names must match. Null denotes no pattern.
     * @return List of matched table names.
     */
    List<String> listTableNames(String pattern);

    /**
     * List the available {@link JsonTable} in the current database that matches an optional pattern.
     * @param pattern Optional pattern that the table names must match. Null denotes no pattern.
     * @return List of matched table.
     */
    List<JsonTable> listTables(String pattern);

    /**
     * Scan table with a desired column name list.
     * @param tableName The table name.
     * @param columns Optional column names. Null denotes all columns are desired.
     * @return The records in the form of {@link JsonNode}.
     * @throws IOException When IO error occurs.
     */
    List<JsonNode> scanTable(String tableName, List<String> columns) throws IOException;

    /**
     * Scan table as stream with a desired column name list.
     * @param tableName The table name.
     * @param columns Optional column names. Null denotes all columns are desired.
     * @return The {@link JsonReader} of the table.
     * @throws IOException When IO error occurs.
     */
    JsonReader scanTableAsStream(String tableName, List<String> columns) throws IOException;;

}
