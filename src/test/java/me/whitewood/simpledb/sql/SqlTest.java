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

package me.whitewood.simpledb.sql;

import com.google.common.collect.Lists;
import me.whitewood.simpledb.adapter.json.JsonAdapterSchema;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Tests for SQLs.
 **/
public class SqlTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTest.class);

    @Test
    public void testParseQuery() {
        Parser testParser = new Parser();
        SqlNode astNode = testParser.parse("select o.is_prepaid, count(distinct u.user_id) as buyer_count " +
                              "from tbl_order o, tbl_user u " +
                              "where o.buyer_id = u.user_id and u.age <= 20 " +
                              "group by o.is_prepaid");
        assertEquals(SqlKind.SELECT, astNode.getKind());

        // simple calcite schema
        final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CalciteSchema calciteSchema = CalciteSchema.createRootSchema(
                true,
                false,
                "json",
                new JsonAdapterSchema(new File("src/test/resources/testdb"), Lists.newArrayList()));
        Properties properties = new Properties();
        properties.setProperty(
                CalciteConnectionProperty.DEFAULT_NULL_COLLATION.camelName(),
                NullCollation.LOW.name());
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteConnectionConfigImpl connectionConfig =
                new CalciteConnectionConfigImpl(properties);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                calciteSchema,
                Lists.newArrayList("json"),
                typeFactory,
                connectionConfig
                );

        // default calcite planner
        Planner planner = Frameworks.getPlanner(Frameworks.newConfigBuilder().build());
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory
                );

        SqlToRelConverter converter = new SqlToRelConverter(
                (PlannerImpl) planner,
                validator,
                catalogReader,
                RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory)),
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.Config.DEFAULT
        );

        RelRoot root = converter.convertQuery(astNode, true, true);
        LOGGER.info(
            RelOptUtil.dumpPlan(
                    "Plan after converting SqlNode to RelNode",
                    root.rel,
                    SqlExplainFormat.TEXT,
                    SqlExplainLevel.EXPPLAN_ATTRIBUTES));
    }
}
