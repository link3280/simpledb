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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.whitewood.simpledb.engine.json.common.JsonTable;
import me.whitewood.simpledb.engine.json.server.JsonDatabaseServer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * EmbeddedJsonDatabaseServer serves request from {@link EmbeddedJsonDatabaseClient}.
 **/
public class EmbeddedJsonDatabaseServer implements JsonDatabaseServer {

    private final EmbeddedJsonDatabaseMaster master;

    private final Executor executor;

    private static final int QUEUE_SIZE = 10;

    public EmbeddedJsonDatabaseServer(String basePath) {
        this.master = new EmbeddedJsonDatabaseMaster(basePath);
        this.executor = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(QUEUE_SIZE, false),
                new ThreadFactoryBuilder().setDaemon(false).setNameFormat("embedded-server-thread-%d").build()
        );
    }

    public EmbeddedJsonDatabaseClient getClient() {
        return new EmbeddedJsonDatabaseClient(this);
    }

    @Override
    public List<String> listTableNames(@Nullable String pattern, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return CompletableFuture.supplyAsync(() -> master.listTableNames(pattern), executor).get(timeout, unit);
    }

    @Override
    public List<JsonTable> listTables(@Nullable String pattern, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return CompletableFuture.supplyAsync(() -> master.listTables(pattern), executor).get(timeout, unit);
    }

    @Override
    public List<JsonNode> scanTable(String tableName, @Nullable List<String> columns, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return master.scanTable(tableName, columns);
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan table " + tableName, e);
            }
        }).get(timeout, unit);
    }

    @Override
    public InputStream scanTableAsStream(String tableName, @Nullable List<String> columns, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return master.scanTableAsStream(tableName, columns);
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan table " + tableName, e);
            }
        }).get(timeout, unit);
    }

}