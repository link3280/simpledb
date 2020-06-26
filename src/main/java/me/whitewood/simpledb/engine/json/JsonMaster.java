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

package me.whitewood.simpledb.engine.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Collections;

/**
 * JsonMaster holds metadata of tables in a database. Since JsonMetaMaster is read only,
 * the metadata is loaded once into memories on startup and never written back.
 *
 * The directory structure of a Json database is like (without partition):
 *
 * ${basePath} - _metadata  - meta.json
 *             - ${table 1} - ${jsonFile 1..N}
 *             - ${table 2} - ${jsonFile 1..N}
 *             - ${table 3} - ${jsonFile 1..N}
 **/
public class JsonMaster {

    private final String basePath;

    private final JsonDatabase database;

    private final Map<String, JsonTable> tableMap = Maps.newHashMap();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int MAX_RESULT_SIZE = 1024;

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonMaster.class);

    public JsonMaster(String basePath) {
        this.basePath = basePath;
        this.database = JsonDatabaseFactory.getJsonDatabase(basePath);
        for (JsonTable table: database.getTables()) {
            this.tableMap.put(table.getName(), table);
        }
    }

    public List<String> listTables() {
        List<String> tableNames = Lists.newArrayList(tableMap.keySet());
        Collections.sort(tableNames);
        return tableNames;
    }

    public JsonTable getTable(String tableName) {
        if (tableMap.containsKey(tableName)) {
            return tableMap.get(tableName);
        } else {
            throw new IllegalArgumentException(String.format("Table %s doesn't exist", tableName));
        }
    }

    public List<JsonNode> scanTable(String tableName) throws IOException {
        File tableDir = new File(basePath, tableName);
        Preconditions.checkState(tableDir.exists(), "Table directory %s doesn't exist", tableDir);
        File[] files = tableDir.listFiles((dir, name) -> name.endsWith(".json"));
        Preconditions.checkNotNull(
                files,
                "Failed to read table %s, for errors while reading table base directory %s",
                tableName,
                tableDir);
        List<JsonNode> result = Lists.newArrayList();
        for (File file: files) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null && result.size() < MAX_RESULT_SIZE) {
                    result.add(OBJECT_MAPPER.readTree(line));
                }
                if (result.size() == MAX_RESULT_SIZE) {
                    LOGGER.warn("Scan query reached limit of result set size, returning the top {} records", MAX_RESULT_SIZE);
                }
            }
        }
        return result;
    }

    public List<JsonNode> scanTable(JsonTable table) throws IOException {
        return scanTable(table.getName());
    }

}
