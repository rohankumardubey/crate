
package io.crate.execution.ddl.tables;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataTypes;

public class TransportAddColumnActionTest extends IntegTestCase {

    @Test
    public void test_can_add_column_to_object_without_providing_top_level_ref() throws Exception {
        execute("create table doc.tbl (o object as (x int))");

        TransportAddColumnAction addColumnAction = internalCluster().getInstance(TransportAddColumnAction.class);
        RelationName relationName = new RelationName("doc", "tbl");
        Reference oy = new SimpleReference(
            new ReferenceIdent(relationName, "o", List.of("y")),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            -1,
            null
        );
        AddColumnRequest request = new AddColumnRequest(relationName, List.of(oy), Map.of(), new IntArrayList(0));
        CompletableFuture<AcknowledgedResponse> result = addColumnAction.execute(request);
        result.get(5, TimeUnit.SECONDS);
    }
}
