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

package io.crate.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import io.crate.expression.NestableInput;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.types.DataTypes;

public class MapBackedRefResolverTest {

    private static final RelationName USERS_TI = new RelationName(Schemas.DOC_SCHEMA_NAME, "users");

    @Test
    public void testGetImplementation() throws Exception {
        ReferenceResolver<NestableInput<?>> refResolver = new MapBackedRefResolver(
            Collections.singletonMap(ColumnIdent.of("obj"), mock(NestableInput.class)));
        NestableInput<?> implementation = refResolver.getImplementation(
            new SimpleReference(
                new ReferenceIdent(USERS_TI, ColumnIdent.of("obj", Arrays.asList("x", "z"))),
                RowGranularity.DOC,
                DataTypes.STRING,
                0,
                null
            ));

        assertThat(implementation).isNull();
    }
}
