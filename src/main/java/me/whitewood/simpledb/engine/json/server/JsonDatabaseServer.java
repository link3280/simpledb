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

package me.whitewood.simpledb.engine.json.server;

import com.fasterxml.jackson.databind.JsonNode;
import me.whitewood.simpledb.engine.json.common.JsonTable;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * JsonDataBaseServer serve request from {@link me.whitewood.simpledb.engine.json.client.JsonDatabaseClient},
 * with the help of {@link JsonDatabaseMaster}, which manages metadata and handles the physical operations.
 **/
public interface JsonDatabaseServer {

    List<String> listTableNames(@Nullable String pattern, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException;

    List<JsonTable> listTables(@Nullable String pattern, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException;

    List<JsonNode> scanTable(String tableName, @Nullable List<String> columns, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException;

    InputStream scanTableAsStream(String tableName, @Nullable List<String> columns, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException;
}
