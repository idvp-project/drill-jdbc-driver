package org.apache.drill.jdbc.impl;


import org.apache.calcite.avatica.*;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.ServerMethod;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.jdbc.impl.DrillFactory;
import org.apache.drill.jdbc.impl.EnhancedDrill41PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Properties;
import java.util.TimeZone;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
public class EnhancedDrillJdbc41Factory extends DrillFactory {

    private static final Logger logger = LoggerFactory.getLogger(DrillJdbc41Factory.class);

    public EnhancedDrillJdbc41Factory() {
        super(4, 1);
    }

    DrillConnectionImpl newDrillConnection(DriverImpl driver,
                                           DrillFactory factory,
                                           String url,
                                           Properties info) throws SQLException {
        return new DrillConnectionImpl(driver, factory, url, info);
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
        return new DrillResultSetImpl(statement, signature, metaData, timeZone, firstFrame);
    }

    public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement, Meta.Signature signature) {
        return new DrillResultSetMetaDataImpl(statement, null, signature);
    }
}
