package org.idvp.drill.jdbc.impl;


import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.drill.jdbc.impl.DrillFactory;
import org.apache.drill.jdbc.impl.EnhancedDrill41PreparedStatement;

import java.sql.SQLException;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
class DrillJdbc41Factory extends DrillFactory {

    @Override
    public EnhancedDrill41PreparedStatement newPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h, Meta.Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return super.newPreparedStatement(connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
    }
}
