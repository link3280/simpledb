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

package me.whitewood.simpledb.engine.json.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;

/**
 * A simple reader that wraps the JSON parsing, with an optional field name list for pushdown filters.
 **/
public class JsonReader implements Closeable {

    private final BufferedReader br;

    @Nullable
    private List<String> fields;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public JsonReader(InputStream in) {
        this(in, null);
    }

    public JsonReader(InputStream in, List<String> fields) {
        this.br = new BufferedReader(new InputStreamReader(in));
        this.fields = fields;
    }

    public JsonReader(Reader in) {
        this.br = new BufferedReader(in);
    }

    public JsonReader(Reader in, List<String> fields) {
        this.br = new BufferedReader(in);
        this.fields = fields;
    }

    public JsonNode readJson() throws IOException {
        String jsonString = br.readLine();
        if (jsonString == null) {
            return null;
        }
        ObjectNode node = (ObjectNode) OBJECT_MAPPER.readTree(jsonString);
        if (fields != null) {
            Iterator<String> ite = node.fieldNames();
            while (ite.hasNext()) {
                String fieldName = ite.next();
                if (!fields.contains(fieldName)) {
                    ite.remove();
                }
            }
        }
        return node;
    }

    @Override
    public void close() throws IOException {
        br.close();
    }
}
