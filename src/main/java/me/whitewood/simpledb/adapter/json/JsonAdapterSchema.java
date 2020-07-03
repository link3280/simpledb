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
import me.whitewood.simpledb.engine.json.JsonMaster;
import me.whitewood.simpledb.engine.json.JsonTable;
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

    /** Base path for the database schema, and each of its child directories holds data of a table. */
    private final File basePath;

    /** Mainly views defined in model.json. */
    private final List<org.apache.calcite.model.JsonTable> tables;

    private final JsonMaster jsonMaster;

    /** Table name to table mapping. **/
    private Map<String, Table> tableMap;

    public JsonAdapterSchema(File basePath, List<org.apache.calcite.model.JsonTable> tables) {
        super();
        this.basePath = basePath;
        this.jsonMaster = new JsonMaster(basePath.getAbsolutePath());
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

    private Map<String, Table> createTableMap() throws IOException {
        Map<String, Table> newTableMap = Maps.newHashMap();
        List<String> tableNames = jsonMaster.listTables();
        for (String tableName : tableNames) {
            JsonTable table = jsonMaster.getTable(tableName);
            newTableMap.put(tableName, new JsonAdapterTable(jsonMaster, table));
        }
        return newTableMap;
    }
}
