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

/**
 * Data types that defined in json-schema. See https://json-schema.org/understanding-json-schema/reference/type.html.
 **/
public enum JsonDataType {

    /** Data type for string and its derived types (eg. regex). */
    STRING,

    /** Boolean type. */
    BOOLEAN,

    /** Generic numeric type (eg. Java int/long/double/float/decimal). */
    NUMBER,

    /** Precise numeric values types, which is subset of NUMBER (eg. Java int/long/byte). */
    INTEGER

    /*
     * TODO: Json array, Json object and null are not supported for now.
     */
}
