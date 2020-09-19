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

package me.whitewood.simpledb.engine.json.embedded;

import com.fasterxml.jackson.databind.JsonNode;
import me.whitewood.simpledb.engine.json.client.JsonDatabaseClient;
import me.whitewood.simpledb.engine.json.client.JsonReader;
import me.whitewood.simpledb.engine.json.common.JsonTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * EmbeddedJsonDatabaseClient interacts with {@link EmbeddedJsonDatabaseMaster} via a blocking queue.
 **/
public class EmbeddedJsonDatabaseClient implements JsonDatabaseClient {

    private final EmbeddedJsonDatabaseServer server;

    private static final int MAX_RESULT_SIZE = 1024;

    private static final long TIMEOUT = 10;

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedJsonDatabaseClient.class);

    public EmbeddedJsonDatabaseClient(EmbeddedJsonDatabaseServer server) {
        this.server = server;
    }

    public List<String> listTableNames() {
        return listTableNames(null);
    }

    @Override
    public List<String> listTableNames(@Nullable String pattern) {
        try {
            return server.listTableNames(pattern, TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get table names. Please retry later.", e);
        }
    }

    @Override
    public List<JsonTable> listTables(@Nullable String pattern) {
        try {
            return server.listTables(pattern, TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get tables. Please retry later.", e);
        }
    }

    public JsonTable getTable(String tableName) {
        try {
            // TODO: replace fuzzy match with exact match
            List<JsonTable> tables = server.listTables(tableName, TIMEOUT, TimeUnit.SECONDS);
            if (tables.size() >= 1) {
                return tables.get(0);
            } else {
                throw  new RuntimeException("Table " + tableName + " does not exist.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get tables. Please retry later.", e);
        }
    }

    @Override
    public List<JsonNode> scanTable(String tableName, @Nullable List<String> columns) throws IOException {
        try {
            return server.scanTable(tableName, columns, TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan table. Please retry later.", e);
        }
    }

    public List<JsonNode> scanTable(String tableName) throws IOException {
        return scanTable(tableName, null);
    }

    public List<JsonNode> scanTable(JsonTable table) throws IOException {
        return scanTable(table.getName());
    }

    /**
     * Scan a table in stream fashion.
     * @param tableName Name of the table.
     * @return JsonReader that reads all the input streams of the files of that table.
     * @throws IOException When an IO error occurs.
     */
    public JsonReader scanTableAsStream(String tableName) throws IOException {
        return scanTableAsStream(tableName, null);
    }

    @Override
    public JsonReader scanTableAsStream(String tableName, List<String> columns) throws IOException {
        try {
            return new JsonReader(server.scanTableAsStream(tableName, columns, TIMEOUT, TimeUnit.SECONDS));
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan table. Please retry later.", e);
        }
    }

}
