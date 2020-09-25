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

import com.google.common.collect.Lists;
import me.whitewood.simpledb.sql.adapter.json.JsonAdapterSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for {@link JsonAdapterSchema}.
 **/
public class JsonAdapterSchemaTest {

    private File baseDire = new File("src/test/resources/testdb");

    private JsonAdapterSchema jsonAdapterSchema = new JsonAdapterSchema(baseDire, null);

    @Test
    public void testCreateTableMap() {
        Map<String, Table> tableMap = jsonAdapterSchema.getTableMap();
        assertEquals(2, tableMap.size());
        RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl();
        RelDataType expectedType = relDataTypeFactory.createStructType(
                Lists.newArrayList(
                        relDataTypeFactory.createSqlType(SqlTypeName.DOUBLE),
                        relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
                        relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
                        relDataTypeFactory.createSqlType(SqlTypeName.BOOLEAN)
                        ),
                Lists.newArrayList("order_id", "buyer_id", "create_time", "is_prepaid"));
        // RelDataType doesn't implement proper equal, so we compare the string representations
        assertEquals(expectedType.toString(), tableMap.get("tbl_order".toUpperCase())
                .getRowType(relDataTypeFactory).toString());
    }
}
