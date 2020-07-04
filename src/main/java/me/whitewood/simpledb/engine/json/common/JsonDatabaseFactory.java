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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;

/**
 * JsonDatabaseFactory creates {@link JsonDatabase} using the given base path of a database.
 **/
public class JsonDatabaseFactory {

    private static final String META_DIR = "_metadata";

    private static final String META_FILE = "meta.json";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static JsonDatabase getJsonDatabase(String basePath) {
        File baseDir = new File(basePath);
        Preconditions.checkState(baseDir.isDirectory(), "Base path %s permission denied or non-existed.", baseDir);
        File metaFile = new File(basePath, META_DIR + File.separator + META_FILE);
        Preconditions.checkState(metaFile.canRead(), "Meta file %s permission denied or non-existed.", metaFile);
        try {
            return OBJECT_MAPPER.readValue(metaFile, JsonDatabase.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read meta file: " + metaFile.getAbsolutePath(), e);
        }
    }
}
