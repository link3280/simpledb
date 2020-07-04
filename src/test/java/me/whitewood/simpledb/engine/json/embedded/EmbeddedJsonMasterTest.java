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
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests for {@link EmbeddedJsonMaster}.
 **/
public class EmbeddedJsonMasterTest {

    private final String testdbPath = "src/test/resources/testdb";

    private final EmbeddedJsonMaster jsonMaster = new EmbeddedJsonMaster(testdbPath);

    @Test
    public void testListTable() {
        List<String> tables = jsonMaster.listTables();
        assertEquals(1, tables.size());
        assertEquals("tbl_order", tables.get(0));
    }

    @Test
    public void testTableScan() throws IOException {
        List<JsonNode> jsonNodeList = jsonMaster.scanTable("tbl_order");
        assertEquals(3, jsonNodeList.size());

        JsonNode record1 = jsonNodeList.get(0);
        assertEquals(5, record1.size());
        assertEquals(JsonNodeType.NUMBER, record1.get("order_id").getNodeType());
        assertEquals(10001, record1.get("order_id").asInt());
        assertEquals(JsonNodeType.NUMBER, record1.get("amount").getNodeType());
        assertTrue(Math.abs(27.53 - record1.get("amount").asDouble()) < 0.01);
        assertEquals(JsonNodeType.STRING, record1.get("buyer_id").getNodeType());
        assertEquals("u234152", record1.get("buyer_id").asText());
        assertEquals(JsonNodeType.BOOLEAN, record1.get("is_prepaid").getNodeType());
        assertEquals(false, record1.get("is_prepaid").asBoolean());
    }

    @Test
    public void testTableScanStream() throws IOException {
        String expected =
                "{\"order_id\":10001, \"buyer_id\":\"u234152\", \"amount\": 27.53, \"create_time\":\"2020-06-29T22:00:11+08:00\", \"is_prepaid\": false}\n" +
                "{\"order_id\":10002, \"buyer_id\":\"u2341534\", \"amount\": 12.3, \"create_time\":\"2020-06-29T22:01:02+08:00\", \"is_prepaid\": true}\n" +
                "{\"order_id\":10003, \"buyer_id\":\"u92742\", \"amount\": 84.3, \"create_time\":\"2020-07-02T09:26:33+08:00\", \"is_prepaid\": true}\n";
        try (
            InputStream inputStream = jsonMaster.scanTableAsStream("tbl_order");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))
        ) {
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            assertEquals(expected, sb.toString());
        }
    }
}
