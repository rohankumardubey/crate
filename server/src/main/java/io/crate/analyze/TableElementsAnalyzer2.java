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

package io.crate.analyze;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.elasticsearch.common.UUIDs;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.ddl.GeoSettingsApplier;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Lists2;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.table.Operation;
import io.crate.planner.operators.EnsureNoMatchPredicate;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.PartitionedBy;
import io.crate.sql.tree.PrimaryKeyColumnConstraint;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.GeoShapeType;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;

public class TableElementsAnalyzer2 implements FieldProvider<Reference> {

    private static final Set<Integer> UNSUPPORTED_INDEX_TYPE_IDS = Set.of(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );
    private static final Set<Integer> UNSUPPORTED_PK_TYPE_IDS = Set.of(
        ObjectType.ID,
        DataTypes.GEO_POINT.id(),
        DataTypes.GEO_SHAPE.id()
    );

    private static final String COLUMN_STORE_PROPERTY = "columnstore";

    private final RelationName table;
    private final ExpressionAnalyzer expressionAnalyzer;
    private final ExpressionAnalysisContext expressionContext;
    /**
     * Columns are in the order as they appear in the statement
     */
    private final Map<ColumnIdent, RefBuilder> columns = new LinkedHashMap<>();
    private final PeekColumns peekColumns;
    private final ColumnAnalyzer columnAnalyzer;
    private final Function<Expression, Symbol> toSymbol;
    private final Set<ColumnIdent> primaryKeys = new HashSet<>();
    private final Map<String, AnalyzedCheck> checks = new LinkedHashMap<>();

    public TableElementsAnalyzer2(RelationName relationName,
                                  CoordinatorTxnCtx txnCtx,
                                  NodeContext nodeCtx,
                                  ParamTypeHints paramTypeHints) {
        this.table = relationName;
        this.expressionAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            paramTypeHints,
            this,
            null
        );
        this.expressionContext = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        this.columnAnalyzer = new ColumnAnalyzer();
        this.peekColumns = new PeekColumns();
        this.toSymbol = x -> expressionAnalyzer.convert(x, expressionContext);
    }

    static class RefBuilder {

        private final ColumnIdent name;
        private DataType<?> type;

        private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;
        private IndexType indexType = IndexType.PLAIN;
        private RowGranularity rowGranularity = RowGranularity.DOC;
        private String indexMethod;
        private boolean nullable = true;
        private GenericProperties<Symbol> indexProperties = GenericProperties.empty();
        private boolean primaryKey;
        private Symbol generated;
        private Symbol defaultExpression;
        private GenericProperties<Symbol> storageProperties = GenericProperties.empty();
        private List<Symbol> indexSources = List.of();


        /**
         * cached result
         **/
        private Reference builtReference;

        public RefBuilder(ColumnIdent name, DataType<?> type) {
            this.name = name;
            this.type = type;
        }

        public boolean isPrimaryKey() {
            return primaryKey;
        }

        public Reference build(Map<ColumnIdent, RefBuilder> columns,
                               RelationName tableName,
                               Function<Symbol, Symbol> bindParameter,
                               Function<Symbol, Object> toValue) {
            if (builtReference != null) {
                return builtReference;
            }

            StorageSupport<?> storageSupport = type.storageSupportSafe();
            Symbol columnStoreSymbol = storageProperties.get(COLUMN_STORE_PROPERTY);
            if (!storageSupport.supportsDocValuesOff() && columnStoreSymbol != null) {
                throw new IllegalArgumentException("Invalid storage option \"columnstore\" for data type \"" + type.getName() + "\"");
            }
            boolean hasDocValues = columnStoreSymbol == null
                ? storageSupport.getComputedDocValuesDefault(indexType)
                : DataTypes.BOOLEAN.implicitCast(toValue.apply(columnStoreSymbol));
            Reference ref;
            ReferenceIdent refIdent = new ReferenceIdent(tableName, name);
            int position = -1;


            if (defaultExpression != null) {
                defaultExpression = bindParameter.apply(defaultExpression);
            }

            if (!indexSources.isEmpty() || indexType == IndexType.FULLTEXT || indexProperties.properties().containsKey("analyzer")) {
                List<Reference> sources = new ArrayList<>(indexSources.size());
                for (Symbol indexSource : indexSources) {
                    if (!ArrayType.unnest(indexSource.valueType()).equals(DataTypes.STRING)) {
                        throw new IllegalArgumentException("INDEX definition only support 'string' typed source columns");
                    }
                    Reference source = (Reference) RefReplacer.replaceRefs(bindParameter.apply(indexSource), x -> {
                        if (x instanceof DynamicReference) {
                            RefBuilder column = columns.get(x.column());
                            return column.build(columns, tableName, bindParameter, toValue);
                        }
                        return x;
                    });
                    if (Reference.indexOf(sources, source.column()) > -1) {
                        throw new IllegalArgumentException("Index " + name + " contains duplicate columns");
                    }
                    sources.add(source);
                }
                String analyzer = DataTypes.STRING.sanitizeValue(indexProperties.map(toValue).get("analyzer"));
                ref = new IndexReference(
                    refIdent,
                    rowGranularity,
                    type,
                    columnPolicy,
                    indexType,
                    nullable,
                    hasDocValues,
                    position,
                    defaultExpression,
                    sources,
                    analyzer == null ? (indexType == IndexType.PLAIN ? "keyword" : "standard") : analyzer
                );
            } else if (type.id() == GeoShapeType.ID) {
                Map<String, Object> geoMap = new HashMap<>();
                GeoSettingsApplier.applySettings(geoMap, indexProperties.map(toValue), indexMethod);
                Float distError = (Float) geoMap.get("distance_error_pct");
                ref = new GeoReference(
                    refIdent,
                    type,
                    columnPolicy,
                    indexType,
                    nullable,
                    position,
                    defaultExpression,
                    indexMethod,
                    (String) geoMap.get("precision"),
                    (Integer) geoMap.get("tree_levels"),
                    distError == null ? null : distError.doubleValue()
                );
            } else {
                ref = new SimpleReference(
                    refIdent,
                    rowGranularity,
                    type,
                    columnPolicy,
                    indexType,
                    nullable,
                    hasDocValues,
                    position,
                    defaultExpression
                );
            }
            if (generated != null) {
                generated = RefReplacer.replaceRefs(bindParameter.apply(generated), x -> {
                    if (x instanceof DynamicReference dynamicRef) {
                        RefBuilder column = columns.get(dynamicRef.column());
                        Reference reference = column.build(columns, tableName, bindParameter, toValue);
                        if (reference instanceof GeneratedReference generatedReference) {
                            throw new ColumnValidationException(name.sqlFqn(), tableName, "a generated column cannot be based on a generated column");
                        }
                    }
                    return x;
                });
                ref = new GeneratedReference(ref, generated.toString(Style.UNQUALIFIED), generated);
            }

            builtReference = ref;
            return ref;
        }
    }

    @Override
    public Reference resolveField(QualifiedName qualifiedName,
                                  @Nullable List<String> path,
                                  Operation operation,
                                  boolean errorOnUnknownObjectKey) {
        var columnIdent = ColumnIdent.fromNameSafe(qualifiedName, path);
        var columnBuilder = columns.get(columnIdent);
        if (columnBuilder == null) {
            throw new ColumnUnknownException(columnIdent, table);
        }
        var dynamicReference = new DynamicReference(new ReferenceIdent(table, columnIdent), RowGranularity.DOC, -1);
        dynamicReference.valueType(columnBuilder.type);
        return dynamicReference;
    }

    public AnalyzedCreateTable analyze(CreateTable<Expression> createTable) {
        for (var tableElement : createTable.tableElements()) {
            tableElement.accept(peekColumns, null);
        }
        for (var tableElement : createTable.tableElements()) {
            tableElement.accept(columnAnalyzer, null);
        }
        GenericProperties<Symbol> properties = createTable.properties().map(toSymbol);
        Optional<ClusteredBy<Symbol>> clusteredBy = createTable.clusteredBy().map(x -> x.map(toSymbol));

        Optional<PartitionedBy<Symbol>> partitionedBy = createTable.partitionedBy().map(x -> x.map(toSymbol));
        partitionedBy.ifPresent(p -> p.columns().forEach(partitionColumn -> {
            ColumnIdent partitionColumnIdent = Symbols.pathFromSymbol(partitionColumn);
            if (partitionColumnIdent.isSystemColumn()) {
                throw new IllegalArgumentException("Cannot use system columns in PARTITIONED BY clause");
            }
            RefBuilder column = columns.get(partitionColumnIdent);
            if (column == null) {
                throw new ColumnUnknownException(partitionColumnIdent, table);
            }
            ensureValidPartitionColumn(clusteredBy, partitionColumnIdent, column);
            column.indexType = IndexType.NONE;
            column.rowGranularity = RowGranularity.PARTITION;
        }));

        return new AnalyzedCreateTable(
            table,
            createTable.ifNotExists(),
            columns,
            checks,
            properties,
            partitionedBy,
            clusteredBy
        );
    }

    private void ensureValidPartitionColumn(Optional<ClusteredBy<Symbol>> clusteredBy, ColumnIdent partitionColumnIdent, RefBuilder column) {
        if (!primaryKeys.isEmpty() && !primaryKeys.contains(partitionColumnIdent)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot use non primary key column '%s' in PARTITIONED BY clause if primary key is set on table",
                partitionColumnIdent.sqlFqn()));
        }
        if (!DataTypes.isPrimitive(column.type)) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot use column %s of type %s in PARTITIONED BY clause",
                partitionColumnIdent.sqlFqn(),
                column.type
            ));
        }
        for (ColumnIdent parent : partitionColumnIdent.parents()) {
            RefBuilder parentColumn = columns.get(parent);
            if (parentColumn.type instanceof ArrayType<?>) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Cannot use array column %s in PARTITIONED BY clause", partitionColumnIdent.sqlFqn()));
            }
        }
        if (column.indexType == IndexType.FULLTEXT) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot use column %s with fulltext index in PARTITIONED BY clause",
                partitionColumnIdent.sqlFqn()
            ));
        }
        clusteredBy.flatMap(ClusteredBy::column).ifPresent(clusteredBySymbol -> {
            ColumnIdent clusteredByColumnIdent = Symbols.pathFromSymbol(clusteredBySymbol);
            if (partitionColumnIdent.equals(clusteredByColumnIdent)) {
                throw new IllegalArgumentException("Cannot use CLUSTERED BY column in PARTITIONED BY clause");
            }
        });
    }

    class PeekColumns extends DefaultTraversalVisitor<Void, Void> {

        @Override
        @SuppressWarnings("unchecked")
        public Void visitColumnDefinition(ColumnDefinition<?> node, Void context) {
            ColumnDefinition<Expression> columnDefinition = (ColumnDefinition<Expression>) node;
            ColumnType<?> type = node.type();
            ColumnIdent columnName = ColumnIdent.fromNameSafe(columnDefinition.ident(), List.of());
            DataType<?> dataType = type == null ? DataTypes.UNDEFINED : DataTypeAnalyzer.convert(type);

            addColumn(columnName, dataType);
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitIndexDefinition(IndexDefinition<?> node, Void context) {
            IndexDefinition<Expression> indexDefinition = (IndexDefinition<Expression>) node;
            String name = indexDefinition.ident();
            ColumnIdent columnName = ColumnIdent.fromNameSafe(name, List.of());
            addColumn(columnName, DataTypes.STRING);
            return null;
        }

        private void addColumn(ColumnIdent columnName, DataType<?> dataType) {
            RefBuilder builder = new RefBuilder(columnName, dataType);
            RefBuilder exists = columns.put(columnName, builder);
            if (exists != null) {
                throw new IllegalArgumentException("column \"" + columnName.sqlFqn() + "\" specified more than once");
            }
            while (dataType instanceof ArrayType<?> arrayType) {
                dataType = arrayType.innerType();
            }
            if (dataType instanceof ObjectType objectType) {
                for (var entry : objectType.innerTypes().entrySet()) {
                    String childName = entry.getKey();
                    ColumnIdent childColumn = ColumnIdent.getChildSafe(columnName, childName);
                    DataType<?> childType = entry.getValue();
                    addColumn(childColumn, childType);
                }
            }
        }
    }

    class ColumnAnalyzer extends DefaultTraversalVisitor<Void, ColumnIdent> {

        @Override
        @SuppressWarnings("unchecked")
        public Void visitColumnDefinition(ColumnDefinition<?> node, ColumnIdent parent) {
            ColumnDefinition<Expression> columnDefinition = (ColumnDefinition<Expression>) node;
            ColumnIdent columnName = parent == null
                ? new ColumnIdent(columnDefinition.ident())
                : ColumnIdent.getChildSafe(parent, columnDefinition.ident());
            RefBuilder builder = columns.get(columnName);

            Expression defaultExpression = columnDefinition.defaultExpression();
            if (defaultExpression != null) {
                if (builder.type.id() == ObjectType.ID) {
                    throw new IllegalArgumentException("Default values are not allowed for object columns: " + columnName);
                }
                Symbol defaultSymbol = expressionAnalyzer.convert(defaultExpression, expressionContext);
                builder.defaultExpression = defaultSymbol.cast(builder.type, CastMode.IMPLICIT);
                RefVisitor.visitRefs(builder.defaultExpression, x -> {
                    throw new UnsupportedOperationException("Columns cannot be used in this context. " +
                        "Maybe you wanted to use a string literal which requires single quotes: '" + x.column().name() + "'");
                });
                EnsureNoMatchPredicate.ensureNoMatchPredicate(defaultSymbol, "Cannot use MATCH in CREATE TABLE statements");
            }

            Expression generatedExpression = columnDefinition.generatedExpression();
            if (generatedExpression != null) {
                builder.generated = expressionAnalyzer.convert(generatedExpression, expressionContext);
                EnsureNoMatchPredicate.ensureNoMatchPredicate(builder.generated, "Cannot use MATCH in CREATE TABLE statements");
                if (builder.type == DataTypes.UNDEFINED) {
                    builder.type = builder.generated.valueType();
                } else {
                    builder.generated = builder.generated.cast(builder.type, CastMode.IMPLICIT);
                }
            }

            for (var constraint : columnDefinition.constraints()) {
                if (constraint instanceof CheckColumnConstraint<Expression> checkConstraint) {
                    Symbol checkSymbol = expressionAnalyzer.convert(checkConstraint.expression(), expressionContext);
                    addCheck(checkConstraint.name(), checkConstraint.expressionStr(), checkSymbol, columnName);
                } else if (constraint instanceof ColumnStorageDefinition<Expression> storageDefinition) {
                    GenericProperties<Symbol> storageProperties = storageDefinition.properties().map(toSymbol);
                    for (String storageProperty : storageProperties.keys()) {
                        if (!COLUMN_STORE_PROPERTY.equals(storageProperty)) {
                            throw new IllegalArgumentException("Invalid STORAGE WITH option `" + storageProperty + "`");
                        }
                    }
                    builder.storageProperties = storageProperties;
                } else if (constraint instanceof IndexColumnConstraint<Expression> indexConstraint) {
                    builder.indexMethod = indexConstraint.indexMethod();
                    builder.indexProperties = indexConstraint.properties().map(toSymbol);
                    builder.indexType = switch (builder.indexMethod.toLowerCase(Locale.ENGLISH)) {
                        case "fulltext" -> IndexType.FULLTEXT;
                        case "off" -> IndexType.NONE;
                        case "plain" -> IndexType.PLAIN;
                        default -> IndexType.PLAIN;
                    };
                    if (builder.indexType == IndexType.FULLTEXT && !DataTypes.STRING.equals(ArrayType.unnest(builder.type))) {
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "Can't use an Analyzer on column %s because analyzers are only allowed on " +
                            "columns of type \"%s\" of the unbound length limit.",
                            columnName.sqlFqn(),
                            DataTypes.STRING.getName()
                        ));
                    }
                    if (builder.indexType != IndexType.PLAIN && UNSUPPORTED_INDEX_TYPE_IDS.contains(builder.type.id())) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "INDEX constraint cannot be used on columns of type \"%s\"", builder.type));
                    }
                } else if (constraint instanceof NotNullColumnConstraint<Expression> notNull) {
                    builder.nullable = false;
                } else if (constraint instanceof PrimaryKeyColumnConstraint<Expression> primaryKey) {
                    markAsPrimaryKey(builder);
                }
            }

            ColumnType<Expression> type = columnDefinition.type();
            while (type instanceof CollectionColumnType collectionColumnType) {
                type = collectionColumnType.innerType();
            }
            if (type instanceof ObjectColumnType<Expression> objectColumnType) {
                builder.columnPolicy = objectColumnType.columnPolicy().orElse(ColumnPolicy.DYNAMIC);
                for (ColumnDefinition<Expression> nestedColumn : objectColumnType.nestedColumns()) {
                    nestedColumn.accept(this, columnName);
                }
            }

            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitPrimaryKeyConstraint(PrimaryKeyConstraint<?> node, ColumnIdent parent) {
            PrimaryKeyConstraint<Expression> pkConstraint = (PrimaryKeyConstraint<Expression>) node;
            List<Expression> pkColumns = pkConstraint.columns();

            for (Expression pk : pkColumns) {
                Symbol pkColumn = toSymbol.apply(pk);
                ColumnIdent columnIdent = Symbols.pathFromSymbol(pkColumn);
                RefBuilder column = columns.get(columnIdent);
                if (column == null) {
                    throw new ColumnUnknownException(columnIdent, table);
                }
                markAsPrimaryKey(column);
            }

            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitIndexDefinition(IndexDefinition<?> node, ColumnIdent parent) {
            IndexDefinition<Expression> indexDefinition = (IndexDefinition<Expression>) node;
            String name = indexDefinition.ident();
            ColumnIdent columnIdent = parent == null ? new ColumnIdent(name) : ColumnIdent.getChildSafe(parent, name);
            RefBuilder builder = columns.get(columnIdent);
            builder.indexMethod = indexDefinition.method();
            builder.indexProperties = indexDefinition.properties().map(toSymbol);
            builder.indexSources = Lists2.map(indexDefinition.columns(), toSymbol);
            builder.indexType = switch (builder.indexMethod.toLowerCase(Locale.ENGLISH)) {
                case "fulltext" -> IndexType.FULLTEXT;
                case "off" -> IndexType.NONE;
                case "plain" -> IndexType.PLAIN;
                default -> IndexType.PLAIN;
            };
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitCheckConstraint(CheckConstraint<?> node, ColumnIdent parent) {
            CheckConstraint<Expression> checkConstraint = (CheckConstraint<Expression>) node;
            Symbol checkSymbol = toSymbol.apply(checkConstraint.expression());
            addCheck(checkConstraint.name(), checkConstraint.expressionStr(), checkSymbol, null);
            return null;
        }
    }

    private void addCheck(@Nullable String constraintName, String expression, Symbol expressionSymbol, @Nullable ColumnIdent column) {
        if (constraintName == null) {
            do {
                constraintName = genUniqueConstraintName(table, column);
            } while (checks.containsKey(constraintName));
        }
        var analyzedCheck = new AnalyzedCheck(expression, expressionSymbol, null);
        AnalyzedCheck exists = checks.put(constraintName, analyzedCheck);
        if (exists != null) {
            throw new IllegalArgumentException(
                "a check constraint of the same name is already declared [" + constraintName + "]");
        }
    }

    private static String genUniqueConstraintName(RelationName table, ColumnIdent column) {
        StringBuilder sb = new StringBuilder(table.fqn().replace(".", "_"));
        if (column != null) {
            sb.append("_").append(column.fqn().replace(".", "_"));
        }
        sb.append("_check_");
        String uuid = UUIDs.dirtyUUID().toString();
        int idx = uuid.lastIndexOf("-");
        sb.append(idx > 0 ? uuid.substring(idx + 1) : uuid);
        return sb.toString();
    }

    private void markAsPrimaryKey(RefBuilder column) {
        column.primaryKey = true;
        ColumnIdent columnName = column.name;
        DataType<?> type = column.type;
        if (type instanceof ArrayType) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Cannot use column \"%s\" with type \"%s\" as primary key", columnName.sqlFqn(), type));
        }
        if (UNSUPPORTED_PK_TYPE_IDS.contains(type.id())) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Cannot use columns of type \"%s\" as primary key", type));
        }
        for (ColumnIdent parent : columnName.parents()) {
            RefBuilder parentColumn = columns.get(parent);
            if (parentColumn.type instanceof ArrayType<?>) {
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH, "Cannot use column \"%s\" as primary key within an array object", columnName.leafName()));
            }
        }
        boolean wasNew = primaryKeys.add(column.name);
        if (!wasNew) {
            throw new IllegalArgumentException("Columns `" + column.name + "` appears twice in primary key constraint");
        }
    }
}