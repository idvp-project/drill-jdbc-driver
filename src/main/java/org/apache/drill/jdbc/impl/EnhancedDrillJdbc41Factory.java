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


import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
@SuppressWarnings("unused")
public class EnhancedDrillJdbc41Factory extends DrillFactory {

    private static final Logger logger = LoggerFactory.getLogger(DrillJdbc41Factory.class);

    public EnhancedDrillJdbc41Factory() {
        this(4, 1);
    }

    EnhancedDrillJdbc41Factory(int major, int minor) {
        super(major, minor);

    }

    @Override
    DrillConnectionImpl newDrillConnection(DriverImpl driver, DrillFactory drillFactory, String s, Properties properties) throws SQLException {
        return new EnhancedDrillConnectionImpl(driver, drillFactory, s, properties);
    }

    public DrillDatabaseMetaDataImpl newDatabaseMetaData(AvaticaConnection connection) {
        return new DrillDatabaseMetaDataImpl((DrillConnectionImpl)connection);
    }

    public DrillStatementImpl newStatement(AvaticaConnection connection,
                                           Meta.StatementHandle h,
                                           int resultSetType,
                                           int resultSetConcurrency,
                                           int resultSetHoldability) {
        return new DrillStatementImpl((DrillConnectionImpl)connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public EnhancedDrill41PreparedStatement newPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h, Meta.Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        DrillConnectionImpl drillConnection = (DrillConnectionImpl)connection;
        return new EnhancedDrill41PreparedStatement(drillConnection, h, signature, null, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public DrillResultSetImpl newResultSet(AvaticaStatement statement, Meta.Signature signature, TimeZone timeZone, Meta.Frame firstFrame) {
        ResultSetMetaData metaData = this.newResultSetMetaData(statement, signature);
        return new EnhancedDrillResultSet(statement, signature, metaData, timeZone, firstFrame);
    }

    public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement, Meta.Signature signature) {
        return new DrillResultSetMetaDataImpl(statement, null, signature);
    }
}
