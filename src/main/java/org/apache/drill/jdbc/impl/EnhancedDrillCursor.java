/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.jdbc.impl;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.jdbc.impl.avatica.SelectSignature;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Objects;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
class EnhancedDrillCursor extends DrillCursor {

    private final DrillCursorAccessor.DrillCursorProxy proxy;
    private final EnhancedDrillConnectionImpl connection;
    private final AvaticaStatement statement;
    private final Meta.Signature signature;

    EnhancedDrillCursor(EnhancedDrillConnectionImpl connection, AvaticaStatement statement, Meta.Signature signature) {
        super(connection, statement, signature);
        proxy = DrillCursorAccessor.INSTANCE.createProxy(this);
        this.connection = connection;
        this.statement = statement;
        this.signature = signature;
    }


    @Override
    void loadInitialSchema() throws SQLException {
        if (this.proxy.isInitialSchemaLoaded()) {
            throw new IllegalStateException("loadInitialSchema() called a second time");
        } else {
            assert !this.proxy.isAfterLastRow() : "afterLastRow already true in loadInitialSchema()";

            assert !this.proxy.isAfterFirstBatch() : "afterLastRow already true in loadInitialSchema()";

            int currentRecordNumber = this.proxy.getCurrentRecordNumber();

            assert -1 == currentRecordNumber : "currentRecordNumber not -1 (is " + currentRecordNumber + ") in loadInitialSchema()";

            RecordBatchLoader currentBatchHolder = this.proxy.getCurrentBatchHolder();

            assert 0 == currentBatchHolder.getRecordCount() : "currentBatchHolder.getRecordCount() not 0 (is " + currentBatchHolder.getRecordCount() + " in loadInitialSchema()";

            DrillCursor.ResultsListener resultsListener = this.proxy.getResultsListener();

            String effectiveSql = signature.sql;
            if (statement instanceof EnhancedDrill41PreparedStatement) {
                EnhancedDrill41PreparedStatement preparedStatement = (EnhancedDrill41PreparedStatement) statement;
                for (TypedValue value : preparedStatement.getParameterValues()) {
                    Object p = value.toJdbc(Calendar.getInstance());
                    if (p instanceof String) {
                        p = "'" + StringEscapeUtils.escapeSql((String) p) + "'";
                    }
                    effectiveSql = effectiveSql.replaceFirst("\\?", Objects.toString(p));
                }
            }

            PlanHandler.PlanHandlerResult result = null;
            if (signature instanceof SelectSignature) {
                PlanHandler handler = new PlanHandler(connection, effectiveSql);
                handler.handle();
                result = handler.getResult();
            }

            if (result != null) {
                connection.getClient().runQuery(result.queryType, result.plan, resultsListener);
            } else {
                connection.getClient().runQuery(UserBitShared.QueryType.SQL, effectiveSql, resultsListener);
            }


            try {
                resultsListener.awaitFirstMessage();
            } catch (InterruptedException var3) {
                Thread.currentThread().interrupt();
                throw new SQLException("Interrupted", var3);
            }

            this.proxy.setReturnTrueForNextCallToNext(true);
            this.proxy.nextRowInternally();
            this.proxy.setInitialSchemaLoaded(true);
        }
    }


    private final static class DrillCursorAccessor {

        final static DrillCursorAccessor INSTANCE = new DrillCursorAccessor();

        private final Field initialSchemaLoadedField;
        private final Field afterLastRowField;
        private final Field afterFirstBatchField;
        private final Field currentRecordNumberField;
        private final Field currentBatchHolderField;
        private final Field statementField;
        private final Field connectionField;
        private final Field signatureField;
        private final Field resultsListenerField;
        private final Field returnTrueForNextCallToNextField;
        private final Method nextRowInternallyMethod;

        private DrillCursorAccessor() {
            try {
                initialSchemaLoadedField = DrillCursor.class.getDeclaredField("initialSchemaLoaded");
                initialSchemaLoadedField.setAccessible(true);
                afterLastRowField = DrillCursor.class.getDeclaredField("afterLastRow");
                afterLastRowField.setAccessible(true);
                afterFirstBatchField = DrillCursor.class.getDeclaredField("afterFirstBatch");
                afterFirstBatchField.setAccessible(true);
                currentRecordNumberField = DrillCursor.class.getDeclaredField("currentRecordNumber");
                currentRecordNumberField.setAccessible(true);
                currentBatchHolderField = DrillCursor.class.getDeclaredField("currentBatchHolder");
                currentBatchHolderField.setAccessible(true);
                statementField = DrillCursor.class.getDeclaredField("statement");
                statementField.setAccessible(true);
                connectionField = DrillCursor.class.getDeclaredField("connection");
                connectionField.setAccessible(true);
                signatureField = DrillCursor.class.getDeclaredField("signature");
                signatureField.setAccessible(true);
                resultsListenerField = DrillCursor.class.getDeclaredField("resultsListener");
                resultsListenerField.setAccessible(true);
                returnTrueForNextCallToNextField = DrillCursor.class.getDeclaredField("returnTrueForNextCallToNext");
                returnTrueForNextCallToNextField.setAccessible(true);
                nextRowInternallyMethod = DrillCursor.class.getDeclaredMethod("nextRowInternally");
                nextRowInternallyMethod.setAccessible(true);
            } catch (NoSuchFieldException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        DrillCursorProxy createProxy(DrillCursor cursor) {
            return new DrillCursorProxy(cursor);
        }

        class DrillCursorProxy {

            private final DrillCursor cursor;

            private DrillCursorProxy(DrillCursor cursor) {
                this.cursor = cursor;
            }

            boolean isInitialSchemaLoaded() {
                try {
                    return (boolean) initialSchemaLoadedField.get(this.cursor);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            @SuppressWarnings("SameParameterValue")
            void setInitialSchemaLoaded(boolean value) {
                try {
                    initialSchemaLoadedField.set(this.cursor, value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            boolean isAfterLastRow() {
                try {
                    return (boolean) afterLastRowField.get(this.cursor);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            boolean isAfterFirstBatch() {
                try {
                    return (boolean) afterFirstBatchField.get(this.cursor);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            int getCurrentRecordNumber() {
                try {
                    return (int) currentRecordNumberField.get(this.cursor);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            RecordBatchLoader getCurrentBatchHolder() {
                try {
                    return (RecordBatchLoader) currentBatchHolderField.get(this.cursor);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            ResultsListener getResultsListener() {
                try {
                    return (ResultsListener) resultsListenerField.get(this.cursor);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            @SuppressWarnings("SameParameterValue")
            void setReturnTrueForNextCallToNext(boolean value) {
                try {
                    returnTrueForNextCallToNextField.set(this.cursor, value);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            void nextRowInternally() {
                try {
                    nextRowInternallyMethod.invoke(this.cursor);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
