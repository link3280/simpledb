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

package me.whitewood.simpledb.sql.adapter.json;

import com.fasterxml.jackson.databind.JsonNode;
import me.whitewood.simpledb.engine.json.common.JsonDataType;
import me.whitewood.simpledb.engine.json.common.JsonTable;
import me.whitewood.simpledb.engine.json.embedded.EmbeddedJsonDatabaseClient;
import org.apache.calcite.linq4j.Enumerator;

import java.io.IOException;
import java.util.List;

/**
 * Enumerator that iterates over a json table. It's not thread safe.
 *
 * Since JsonClient only supports batch query, JsonEnumerator loads the whole result into memory and
 * provides random access. It's inefficient and impractical for real world use cases, but convenient
 * for experimental uses.
 *
 * TODO: support stream-based IO
 **/
public class JsonEnumerator implements Enumerator<Object[]> {

    private final EmbeddedJsonDatabaseClient jsonClient;

    private final JsonTable jsonTable;

    private final List<String> columnNames;

    private final List<JsonDataType> columnTypes;

    private List<JsonNode> resultSet;

    private int index = -1;

    public JsonEnumerator(EmbeddedJsonDatabaseClient jsonClient, JsonTable jsonTable, List<String> columnNames, List<JsonDataType> columnTypes) {
        this.jsonClient = jsonClient;
        this.jsonTable = jsonTable;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        try {
            resultSet = jsonClient.scanTable(jsonTable);
        } catch (IOException e) {
            throw new RuntimeException("Failed to query table " + jsonTable.getName(), e);
        }
    }

    @Override
    public Object[] current() {
        if (index < 0) {
            throw new IllegalStateException("moveNext() must be called before getting the first element.");
        }
        JsonNode currentNode = resultSet.get(index);
        Object[] row = new Object[columnNames.size()];
        for (int i=0;i<columnNames.size();i++) {
            JsonNode fieldNode = currentNode.get(columnNames.get(i));
            JsonDataType jsonDataType = columnTypes.get(i);
            switch (jsonDataType) {
                case STRING:
                    row[i] = fieldNode.asText();
                    break;
                case NUMBER:
                    row[i] = fieldNode.asDouble();
                    break;
                case INTEGER:
                    row[i] = fieldNode.asInt();
                    break;
                case BOOLEAN:
                    row[i] = fieldNode.asBoolean();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported JSON type: " + jsonDataType);

            }
        }
        return row;
    }

    @Override
    public boolean moveNext() {
        if (index < resultSet.size() - 1) {
            index++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void reset() {
        index = -1;
    }

    @Override
    public void close() {
        resultSet = null;
    }
}
