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

package io.crate.metadata.pgcatalog;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.OID;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.isArray;

import io.crate.expression.reference.information.ColumnContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.Regclass;

public class PgAttributeTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_attribute");

    public static SystemTable<ColumnContext> create() {
        return SystemTable.<ColumnContext>builder(IDENT)
            .add("attrelid", OID, c -> Regclass.relationOid(c.relation()).oid())
            .add("attname", STRING, c -> c.ref().column().sqlFqn())
            .add("atttypid", OID, c -> PGTypes.get(c.ref().valueType()).oid())
            .add("attstattarget", INTEGER, c -> 0)
            .add("attlen", SHORT, c -> PGTypes.get(c.ref().valueType()).typeLen())
            .add("attnum", INTEGER, c -> c.ref().position())
            .add("attndims", INTEGER, c -> isArray(c.ref().valueType()) ? 1 : 0)
            .add("attcacheoff", INTEGER, c -> -1)
            .add("atttypmod", INTEGER, c -> PGTypes.get(c.ref().valueType()).typeMod())
            .add("attbyval", BOOLEAN, c -> false)
            .add("attstorage", STRING, c -> null)
            .add("attalign", STRING, c -> null)
            .add("attnotnull", BOOLEAN, c -> !c.ref().isNullable())
            .add("atthasdef", BOOLEAN, c -> false) // don't support default values
            .add("attidentity", STRING, c -> "")
            .add("attisdropped", BOOLEAN, c -> false) // don't support dropping columns
            .add("attislocal", BOOLEAN, c -> true)
            .add("attinhcount", INTEGER, c -> 0)
            .add("attcollation", OID, c -> 0)
            .add("attacl", new ArrayType<>(DataTypes.UNTYPED_OBJECT), c -> null)
            .add("attoptions", STRING_ARRAY, c -> null)
            .add("attfdwoptions", STRING_ARRAY, c -> null)
            .build();
    }
}
