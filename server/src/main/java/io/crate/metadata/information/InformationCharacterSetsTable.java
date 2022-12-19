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

package io.crate.metadata.information;

import static io.crate.types.DataTypes.STRING;

import io.crate.Constants;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public final class InformationCharacterSetsTable {

    public static final String NAME = "character_sets";
    public static final RelationName IDENT = RelationName.of(InformationSchemaInfo.NAME, NAME);

    public static SystemTable<Void> create() {
        return SystemTable.<Void>builder(IDENT)
            .add("character_set_catalog", STRING, ignored -> null)
            .add("character_set_schema", STRING, ignored -> null)
            .add("character_set_name", STRING, ignored -> "UTF8")
            .add("character_repertoire", STRING, ignored -> "UCS")
            .add("form_of_use", STRING, ignored -> "UTF8")
            .add("default_collate_catalog", STRING, ignored -> Constants.DB_NAME)
            .add("default_collate_schema", STRING, ignored -> null)
            .add("default_collate_name", STRING, ignored -> null)
            .build();
    }
}
