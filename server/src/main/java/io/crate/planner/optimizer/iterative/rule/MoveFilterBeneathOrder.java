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

package io.crate.planner.optimizer.iterative.rule;

import static io.crate.planner.optimizer.iterative.rule.Pattern.typeOf;
import static io.crate.planner.optimizer.iterative.rule.Patterns.source;
import static io.crate.planner.optimizer.iterative.rule.Util.transpose;

import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.Order;
import io.crate.planner.optimizer.iterative.Lookup;
import io.crate.statistics.TableStats;

/**
 * Transforms
 *
 * <pre>
 *     Filter
 *     |
 *     Order
 *     |
 *     X
 * </pre>
 *
 * into
 *
 * <pre>
 *     Order
 *     |
 *     Filter
 *     |
 *     X
 * </pre>
 *
 * Which is always safe to do as Order doesn't produce new outputs and the order operation becomes cheaper if we can
 * remove data up-front.
 */
public final class MoveFilterBeneathOrder implements Rule<Filter> {

    private final Capture<Order> orderCapture;
    private final Pattern<Filter> pattern;

    public MoveFilterBeneathOrder() {
        this.orderCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(), typeOf(Order.class).capturedAs(orderCapture));
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Lookup lookup) {
        return transpose(filter, captures.get(orderCapture), lookup);
    }
}