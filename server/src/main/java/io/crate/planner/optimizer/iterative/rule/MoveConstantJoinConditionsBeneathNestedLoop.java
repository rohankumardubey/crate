/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.optimizer.iterative.rule;

import static io.crate.planner.optimizer.iterative.rule.FilterOnJoinsUtil.getNewSource;
import static io.crate.planner.optimizer.iterative.rule.Pattern.typeOf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.consumer.RelationNameCollector;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.NestedLoopJoin;
import io.crate.planner.optimizer.iterative.Lookup;
import io.crate.statistics.TableStats;

public class MoveConstantJoinConditionsBeneathNestedLoop implements Rule<NestedLoopJoin> {

    private final Pattern<NestedLoopJoin> pattern;

    public MoveConstantJoinConditionsBeneathNestedLoop() {
        this.pattern = typeOf(NestedLoopJoin.class)
            .with(nl -> nl.joinType() == JoinType.INNER &&
                        !nl.isJoinConditionOptimised() &&
                        nl.joinCondition() != null);
    }

    @Override
    public Pattern<NestedLoopJoin> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(NestedLoopJoin nl,
                             io.crate.planner.optimizer.iterative.rule.Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Lookup lookup) {
        var conditions = nl.joinCondition();
        var allConditions = QuerySplitter.split(conditions);
        var constantConditions = new HashMap<Set<RelationName>, Symbol>(allConditions.size());
        var nonConstantConditions = new HashSet<Symbol>(allConditions.size());
        for (var condition : allConditions.entrySet()) {
            if (numberOfRelationsUsed(condition.getValue()) <= 1) {
                constantConditions.put(condition.getKey(), condition.getValue());
            } else {
                nonConstantConditions.add(condition.getValue());
            }
        }
        if (constantConditions.isEmpty() || nonConstantConditions.isEmpty()) {
            // Nothing to optimize, just mark nestedLoopJoin to skip the rule the next time
            return new NestedLoopJoin(
                nl.lhs(),
                nl.rhs(),
                nl.joinType(),
                nl.joinCondition(),
                nl.isFiltered(),
                nl.topMostLeftRelation(),
                nl.orderByWasPushedDown(),
                nl.isRewriteFilterOnOuterJoinToInnerJoinDone(),
                true, // Mark joinConditionOptimised = true
                nl.id()
            );
        } else {
            // Push constant join condition down to source
            var lhs = lookup.resolve(nl.lhs());
            var rhs = lookup.resolve(nl.rhs());
            var queryForLhs = constantConditions.remove(lhs.getRelationNames());
            var queryForRhs = constantConditions.remove(rhs.getRelationNames());
            var newLhs = getNewSource(queryForLhs, lhs, txnCtx.idAllocator());
            var newRhs = getNewSource(queryForRhs, rhs, txnCtx.idAllocator());
            return new HashJoin(
                newLhs,
                newRhs,
                AndOperator.join(nonConstantConditions),
                nl.id()
            );
        }
    }

    private static int numberOfRelationsUsed(Symbol joinCondition) {
        return RelationNameCollector.collect(joinCondition).size();
    }
}