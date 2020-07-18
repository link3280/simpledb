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

import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * Test for {@link JsonAdapterSchemaFactory}, via JDBC driver.
 **/
public class JsonAdapterTest {

    @Test
    public void testScan() throws SQLException {
        File file = new File("src/test/resources/testdb");
        assertTrue(file.exists());

        String model = "{\n" +
                "  version: '1.0',\n" +
                "  defaultSchema: 'eshop',\n" +
                "  schemas: [\n" +
                "    {\n" +
                "      name: 'eshop',\n" +
                "      type: 'custom',\n" +
                "      factory: '"+ JsonAdapterSchemaFactory.class.getName() +"',\n" +
                "      operand: {\n" +
                "        directory: " + escapeString(file.getAbsolutePath()) + "\n" +
                "      }\n" +
                "   }\n" +
                "  ]\n" +
                "}";

        String sql = "select * from tbl_order";
        try (Connection connection =
                     DriverManager.getConnection("jdbc:calcite:model=inline:" + model + ";caseSensitive=false");
            final CalciteConnection calciteConnection =
                    connection.unwrap(CalciteConnection.class);
            final PreparedStatement statement =
                    calciteConnection.prepareStatement(sql);
            ResultSet rs = statement.executeQuery()) {
            if(rs.next()) {
                assertEquals(10001, rs.getInt("order_id"));
                assertEquals("u234152", rs.getString("buyer_id"));
                assertEquals(false, rs.getBoolean("is_prepaid"));
            }
        }
    }

    /**
     *  Quotes a string for Java or JSON, into a builder.
     *  Modified from Calcite.
     * */
    private static String escapeString(String s) {
        StringBuilder buf = new StringBuilder();
        buf.append('"');
        int n = s.length();
        char lastChar = 0;
        for (int i = 0; i < n; ++i) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    buf.append("\\\\");
                    break;
                case '"':
                    buf.append("\\\"");
                    break;
                case '\n':
                    buf.append("\\n");
                    break;
                case '\r':
                    if (lastChar != '\n') {
                        buf.append("\\r");
                    }
                    break;
                default:
                    buf.append(c);
                    break;
            }
            lastChar = c;
        }
        return buf.append('"').toString();
    }
}
