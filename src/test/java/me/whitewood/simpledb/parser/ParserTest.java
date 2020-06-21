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

package me.whitewood.simpledb.parser;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for {@link Parser}
 **/
public class ParserTest {

    @Test
    public void testParseQuery() {
        Parser testParser = new Parser();
        SqlNode astNode = testParser.parse("select o.category, count(distinct u.buyer) as buyer_count " +
                              "from tb_order o, tbl_user u " +
                              "where o.buyer_id = u.user_id and u.age <= 30 " +
                              "group by o.category");
        assertEquals(SqlKind.SELECT, astNode.getKind());

        SqlSelect select = (SqlSelect) astNode;
        assertFalse(select.isDistinct());

    }
}
