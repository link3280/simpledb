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

package me.whitewood.simpledb.adapter.json;

import com.google.common.collect.Maps;
import me.whitewood.simpledb.engine.json.common.JsonTable;
import me.whitewood.simpledb.engine.json.embedded.EmbeddedJsonDatabaseClient;
import me.whitewood.simpledb.engine.json.embedded.EmbeddedJsonDatabaseServer;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * JsonAdapterSchema that describes a specific schemas for a set of tables.
 * Distinguished from {@link org.apache.calcite.model.JsonSchema}, which represents a serialization approach of metadata,
 * JsonAdapterSchema describes a namespace(database) of JsonAdapter.
 **/
public class JsonAdapterSchema extends AbstractSchema {

    /** Mainly views defined in model.json. */
    private final List<org.apache.calcite.model.JsonTable> tables;

    /** Adaptee **/
    private final EmbeddedJsonDatabaseClient jsonDbClient;

    /** Table name to table mapping. **/
    private Map<String, Table> tableMap;

    public JsonAdapterSchema(File basePath, List<org.apache.calcite.model.JsonTable> tables) {
        super();
        EmbeddedJsonDatabaseServer server = new EmbeddedJsonDatabaseServer(basePath.getAbsolutePath());
        this.jsonDbClient = server.getClient();
        this.tables = tables;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            try {
                tableMap = createTableMap();
            } catch (IOException e) {
                throw new RuntimeException("Failed to initiate database metadata", e);
            }
        }
        return tableMap;
    }

    synchronized private Map<String, Table> createTableMap() throws IOException {
        // last check to ensure table map is not initialized
        if (tableMap != null) {
            return tableMap;
        }
        Map<String, Table> newTableMap = Maps.newHashMap();
        List<String> tableNames = jsonDbClient.listTableNames();
        for (String tableName : tableNames) {
            JsonTable table = jsonDbClient.getTable(tableName);
            newTableMap.put(tableName, new JsonAdapterTable(jsonDbClient, table));
        }
        return newTableMap;
    }
}
