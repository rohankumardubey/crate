/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.dml;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.junit.Test;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;

public class IndexerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_index_object_with_dynamic_column_creation() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(o)
        );

        Map<String, Object> value = Map.of("x", 10, "y", 20);
        ParsedDocument parsedDoc = indexer.index("id1", new Object[] { value });
        assertThat(parsedDoc.doc().getFields())
            .hasSize(10);

        assertThat(parsedDoc.newColumns())
            .hasSize(1);

        assertThat(parsedDoc.source().utf8ToString()).isIn(
            "{\"o\":{\"x\":10,\"y\":20}}",
            "{\"o\":{\"y\":20,\"x\":10}}"
        );
    }

    @Test
    public void test_create_dynamic_object_with_nested_columns() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(o)
        );

        Map<String, Object> value = Map.of("x", 10, "obj", Map.of("y", 20));
        ParsedDocument parsedDoc = indexer.index("id1", new Object[] { value });
        assertThat(parsedDoc.doc().getFields())
            .hasSize(10);

        assertThat(parsedDoc.newColumns())
            .satisfiesExactly(
                col1 -> assertThat(col1).isReference("o['obj']"),
                col2 -> assertThat(col2).isReference("o['obj']['y']")
            );
    }

    @Test
    public void test_create_dynamic_array() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(o)
        );

        Map<String, Object> value = Map.of("x", 10, "xs", List.of(2, 3, 4));
        ParsedDocument parsedDoc = indexer.index("id1", new Object[] { value });

        assertThat(parsedDoc.newColumns())
            .satisfiesExactly(
                col1 -> assertThat(col1).isReference("o['xs']", new ArrayType<>(DataTypes.INTEGER))
            );

        assertThat(parsedDoc.source().utf8ToString()).isIn(
            "{\"o\":{\"x\":10,\"xs\":[2,3,4]}}",
            "{\"o\":{\"xs\":[2,3,4],\"x\":10}}"
        );

        assertThat(parsedDoc.doc().getFields())
            .hasSize(14);
    }

    @Test
    public void test_adds_default_values() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int default 0)")
            .build();
        CoordinatorTxnCtx txnCtx = new CoordinatorTxnCtx(executor.getSessionSettings());
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("x"));
        Reference y = table.getReference(new ColumnIdent("y"));
        var indexer = new Indexer(
            table,
            txnCtx,
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(y)
        );
        var parsedDoc = indexer.index("id1", new Object[] { null });
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"y\":0}"
        );
        assertThat(parsedDoc.doc().getFields())
            .hasSize(8);

        indexer = new Indexer(
            table,
            txnCtx,
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x)
        );
        parsedDoc = indexer.index("id1", 10);
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"x\":10,\"y\":0}"
        );
        assertThat(parsedDoc.doc().getFields())
            .hasSize(10);
    }

    @Test
    public void test_adds_generated_column() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (x int, y int as x + 2)")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("x"));
        Indexer indexer = new Indexer(
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x)
        );
        var parsedDoc = indexer.index("id1", 1);
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"x\":1,\"y\":3}"
        );
    }

    @Test
    public void test_generated_partitioned_column_is_not_indexed_or_included_in_source() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addPartitionedTable("create table tbl (x int, p int as x + 2) partitioned by (p)")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference x = table.getReference(new ColumnIdent("x"));
        Indexer indexer = new Indexer(
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(x)
        );
        var parsedDoc = indexer.index("id1", 1);
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"x\":1}"
        );
    }

    @Test
    public void test_default_and_generated_column_within_object() throws Exception {
        SQLExecutor executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int default 0, y int as o['x'] + 2, z int))")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(o)
        );

        var parsedDoc = indexer.index("id1", Map.of("z", 20));
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"o\":{\"x\":0,\"y\":2,\"z\":20}}"
        );
    }

    @Test
    public void test_default_for_full_object() throws Exception {
        var executor = SQLExecutor.builder(clusterService)
            .addTable("create table tbl (o object as (x int) default {x=10})")
            .build();
        DocTableInfo table = executor.resolveTableInfo("tbl");
        Reference o = table.getReference(new ColumnIdent("o"));
        Indexer indexer = new Indexer(
            table,
            new CoordinatorTxnCtx(executor.getSessionSettings()),
            executor.nodeCtx,
            column -> NumberFieldMapper.FIELD_TYPE,
            List.of(o)
        );
        var parsedDoc = indexer.index("id1", new Object[] { null });
        assertThat(parsedDoc.source().utf8ToString()).isEqualTo(
            "{\"o\":{\"x\":10}}"
        );
    }
}