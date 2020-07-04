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

package me.whitewood.simpledb.engine.json.common;

import java.util.List;

/**
 * POJO that matches meta.json.
 **/
public class JsonDatabase {

    private String name;

    private List<JsonTable> tables;

    public JsonDatabase() {
    }

    public JsonDatabase(String name, List<JsonTable> tables) {
        this.name = name;
        this.tables = tables;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<JsonTable> getTables() {
        return tables;
    }

    public void setTables(List<JsonTable> tables) {
        this.tables = tables;
    }

    @Override
    public String toString() {
        return "JsonDatabase{" +
                "name='" + name + '\'' +
                ", tables=" + tables +
                '}';
    }
}
