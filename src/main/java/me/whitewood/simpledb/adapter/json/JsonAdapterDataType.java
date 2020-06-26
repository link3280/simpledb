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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Map;

/**
 * Date types that defined in json-schema. See json-schema.org.
 **/
enum JsonAdapterDataType {

    /** Json string */
    STRING(String.class, "string"),

    /** Json boolean */
    BOOLEAN(Primitive.BOOLEAN, "boolean"),

    /** Json number */
    NUMBER(Primitive.DOUBLE, "number");

    /*
     * TODO: Json array, map and null are not supported for now.
     */

    /** Physical Java class of a data type */
    private final Class clazz;

    /** Name of a data type */
    private final String name;

    static Map<String, JsonAdapterDataType> MAP = ImmutableMap.of(
            JsonAdapterDataType.STRING.name, JsonAdapterDataType.STRING,
            JsonAdapterDataType.BOOLEAN.name, JsonAdapterDataType.BOOLEAN,
            JsonAdapterDataType.NUMBER.name, JsonAdapterDataType.NUMBER
    );

    /**
     * Convenient method for getting java primitive classes.
     * @param primitive Primitive types defined in `calcite.linq4j`.
     */
    JsonAdapterDataType(Primitive primitive, String name) {
        this(primitive.boxClass, name);
    }

    JsonAdapterDataType(Class clazz, String name) {
        this.clazz = clazz;
        this.name = name;
    }

    public JsonAdapterDataType of(String name) {
        return MAP.get(name);
    }

    public RelDataType toType(JavaTypeFactory typeFactory) {
        RelDataType javaType = typeFactory.createJavaType(clazz);
        RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
        return typeFactory.createTypeWithNullability(sqlType, true);
    }

}
