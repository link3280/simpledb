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

import com.google.common.collect.Lists;
import me.whitewood.simpledb.engine.json.common.JsonColumn;
import me.whitewood.simpledb.engine.json.common.JsonDataType;
import me.whitewood.simpledb.engine.json.common.JsonTable;
import me.whitewood.simpledb.engine.json.embedded.EmbeddedJsonMaster;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Base table that represents a generic table of json adapter.
 **/
public class JsonAdapterTable extends AbstractTable implements ScannableTable {

    private final EmbeddedJsonMaster jsonMaster;

    private final JsonTable jsonTable;

    private RelDataType rowType;

    /**
     * default row type
     */
    private RelProtoDataType protoRowType;

    private final List<String> columnNames = Lists.newArrayList();

    private final List<JsonDataType> columnTypes = Lists.newArrayList();

    JsonAdapterTable(EmbeddedJsonMaster jsonMaster, JsonTable jsonTable) {
        super();
        this.jsonMaster = jsonMaster;
        this.jsonTable = jsonTable;
        for (JsonColumn column : jsonTable.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }
        if (rowType == null) {
            List<RelDataType> columnRelTypes = columnTypes.stream()
                    .map(c -> JsonAdapterDataType.valueOf(c.name().toUpperCase()).toType((JavaTypeFactory) typeFactory))
                    .collect(Collectors.toList());
            rowType = typeFactory.createStructType(columnRelTypes, columnNames);
        }
        return rowType;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new JsonEnumerator(jsonMaster, jsonTable, columnNames, columnTypes);
            }
        };
    }
}
