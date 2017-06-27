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
import org.apache.calcite.avatica.util.Cursor;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
class EnhancedDrillResultSet extends DrillResultSetImpl {
    private final EnhancedDrillConnectionImpl connection;

    EnhancedDrillResultSet(AvaticaStatement statement, Meta.Signature signature, ResultSetMetaData resultSetMetaData, TimeZone timeZone, Meta.Frame firstFrame) {
        super(statement, signature, resultSetMetaData, timeZone, firstFrame);
        this.connection = (EnhancedDrillConnectionImpl) statement.connection;
    }

    @Override
    protected DrillResultSetImpl execute() throws SQLException {
        this.connection.getDriver().handler.onStatementExecute(this.statement, null);
        if (this.signature.cursorFactory != null) {
            super.execute();
            List<Cursor.Accessor> wrappedAccessorList = new ArrayList<>(this.accessorList.size());

            for (Cursor.Accessor accessor : this.accessorList) {
                wrappedAccessorList.add(new WrappedAccessor(accessor));
            }

            this.accessorList = wrappedAccessorList;
        } else {
            EnhancedDrillCursor drillCursor = new EnhancedDrillCursor(this.connection, this.statement, this.signature);
            super.execute2(drillCursor, this.signature.columns);
            drillCursor.loadInitialSchema();
        }

        return this;
    }
}
