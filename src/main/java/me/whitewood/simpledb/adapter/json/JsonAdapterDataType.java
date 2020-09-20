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

import com.google.common.collect.ImmutableMap;
import me.whitewood.simpledb.engine.json.common.JsonDataType;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Map;

/**
 * JsonAdapterDataType is a intermediate type used for converting
 * {@link me.whitewood.simpledb.engine.json.common.JsonDataType} to {@link RelDataType}.
 **/
enum JsonAdapterDataType {

    /** Json string */
    STRING(String.class, JsonDataType.STRING),

    /** Json boolean */
    BOOLEAN(Primitive.BOOLEAN, JsonDataType.BOOLEAN),

    /** Json number */
    NUMBER(Primitive.DOUBLE, JsonDataType.NUMBER),

    /** Json integer */
    INTEGER(Primitive.INT, JsonDataType.INTEGER);

    /*
     * TODO: Json array, map and null are not supported for now.
     */

    /** Physical Java class of a data type */
    private final Class clazz;

    /** Name of a data type */
    private final JsonDataType jsonDataType;

    static Map<JsonDataType, JsonAdapterDataType> MAP = ImmutableMap.of(
            JsonAdapterDataType.STRING.jsonDataType, JsonAdapterDataType.STRING,
            JsonAdapterDataType.BOOLEAN.jsonDataType, JsonAdapterDataType.BOOLEAN,
            JsonAdapterDataType.NUMBER.jsonDataType, JsonAdapterDataType.NUMBER,
            JsonAdapterDataType.INTEGER.jsonDataType, JsonAdapterDataType.INTEGER
    );

    /**
     * Convenient method for getting java primitive classes.
     * @param primitive Primitive types defined in `calcite.linq4j`.
     */
    JsonAdapterDataType(Primitive primitive, JsonDataType jsonDataType) {
        this(primitive.boxClass, jsonDataType);
    }

    JsonAdapterDataType(Class clazz, JsonDataType jsonDataType) {
        this.clazz = clazz;
        this.jsonDataType = jsonDataType;
    }

    public static JsonAdapterDataType of(JsonDataType name) {
        return MAP.get(name);
    }

    /**
     * Specify both the physical type and the logical type of the Json record.
     * @param typeFactory TypeFactory that maps the java class to RelDataType.
     * @return RelDataType.
     */
    public RelDataType toType(JavaTypeFactory typeFactory) {
        RelDataType javaType = typeFactory.createJavaType(clazz);
        RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
        return typeFactory.createTypeWithNullability(sqlType, true);
    }

}
