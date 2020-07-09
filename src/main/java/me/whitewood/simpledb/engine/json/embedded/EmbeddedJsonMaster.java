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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import me.whitewood.simpledb.engine.json.client.JsonDatabaseClient;
import me.whitewood.simpledb.engine.json.client.JsonReader;
import me.whitewood.simpledb.engine.json.common.JsonDatabase;
import me.whitewood.simpledb.engine.json.common.JsonDatabaseFactory;
import me.whitewood.simpledb.engine.json.common.JsonTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * EmbeddedJsonMaster holds metadata of tables in a database. Since json databases are read only,
 * metadata are loaded once into memories on initiation and never written back.
 *
 * The directory structure of a Json database is like (without partition):
 *
 * ${basePath} - _metadata  - meta.json
 *             - ${table 1} - ${jsonFile 1..N}
 *             - ${table 2} - ${jsonFile 1..N}
 *             - ${table 3} - ${jsonFile 1..N}
 **/
public class EmbeddedJsonMaster implements JsonDatabaseClient {

    private final String basePath;

    private final JsonDatabase database;

    private final Map<String, JsonTable> tableMap = Maps.newHashMap();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int MAX_RESULT_SIZE = 1024;

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedJsonMaster.class);

    public EmbeddedJsonMaster(String basePath) {
        this.basePath = basePath;
        this.database = JsonDatabaseFactory.getJsonDatabase(basePath);
        for (JsonTable table: database.getTables()) {
            this.tableMap.put(table.getName(), table);
        }
    }

    public List<String> listTableNames() {
        return listTableNames(null);
    }

    @Override
    public List<String> listTableNames(@Nullable String pattern) {
        if (pattern == null) {
            return tableMap.keySet().stream().sorted().collect(Collectors.toList());
        }
        return tableMap.keySet().stream().filter(
                k -> { Matcher m = Pattern.compile(pattern).matcher(k); return m.find();}
        ).sorted().collect(Collectors.toList());
    }

    @Override
    public List<JsonTable> listTables(@Nullable String pattern) {
        List<String> tableNames = listTableNames(pattern);
        return tableNames.stream().map(tableMap::get).collect(Collectors.toList());
    }


    public JsonTable getTable(String tableName) {
        if (tableMap.containsKey(tableName)) {
            return tableMap.get(tableName);
        } else {
            throw new IllegalArgumentException(String.format("Table %s doesn't exist", tableName));
        }
    }

    @Override
    public List<JsonNode> scanTable(String tableName, @Nullable List<String> columns) throws IOException {
        File[] files = getTableFiles(tableName);
        List<JsonNode> result = Lists.newArrayList();
        for (File file: files) {
            try (JsonReader br = new JsonReader(new FileReader(file), columns)) {
                JsonNode jsonNode;
                while ((jsonNode = br.readJson()) != null && result.size() < MAX_RESULT_SIZE) {
                    result.add(jsonNode);
                }
                if (result.size() == MAX_RESULT_SIZE) {
                    LOGGER.warn("Scan query reached limit of result set size, returning the top {} records", MAX_RESULT_SIZE);
                }
            }
        }
        return result;
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
        File[] files = getTableFiles(tableName);
        List<InputStream> inputStreams = Lists.newArrayList();
        for (File file: files) {
            inputStreams.add(new FileInputStream(file));
        }
        InputStream sequenceInputStream = new SequenceInputStream(Collections.enumeration(inputStreams));
        return new JsonReader(sequenceInputStream, columns);
    }

    @Override
    public JsonDatabase getDatabaseMeta() {
        return database;
    }

    @Nonnull
    private File[] getTableFiles(String tableName) {
        File tableDir = new File(basePath, tableName);
        Preconditions.checkState(tableDir.exists(), "Table directory %s doesn't exist", tableDir);
        File[] files =  tableDir.listFiles((dir, name) -> name.endsWith(".json"));
        Preconditions.checkNotNull(
                files,
                "Failed to read table %s, for errors while reading table base directory %s",
                tableName,
                tableDir);
        return files;
    }
}
