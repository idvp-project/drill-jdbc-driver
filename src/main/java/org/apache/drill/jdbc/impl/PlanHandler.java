package org.apache.drill.jdbc.impl;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.Meta;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.RpcException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
class PlanHandler {
    private final EnhancedDriverImpl driver;
    private final EnhancedDrillConnectionImpl connection;
    private final Meta.Signature signature;
    private PlanHandlerResult result;

    PlanHandler(EnhancedDrillConnectionImpl connection,
                Meta.Signature signature) {

        this.driver = (EnhancedDriverImpl) connection.getDriver();
        this.connection = connection;
        this.signature = signature;
    }


    void handle() throws SQLException {
        try {

            EnhancedDrillConnectionConfig config = connection.getEnhancedConfig();
            if (!config.enabled()) {
                result = new PlanHandlerResult(UserBitShared.QueryType.SQL, signature);
                return;
            }
            if (!config.enabledWithoutParameters() && CollectionUtils.isEmpty(signature.parameters)) {
                result = new PlanHandlerResult(UserBitShared.QueryType.SQL, signature);
                return;
            }

            Meta.Signature planSignature = connection.getCache().getPlan(connection, signature.sql);
            if (planSignature == null) {
                planSignature = createPlan(signature);
                if (planSignature != null) {
                    connection.getCache().storePlan(connection, signature.sql, planSignature);
                    result = new PlanHandlerResult(UserBitShared.QueryType.PHYSICAL, planSignature);
                    return;
                }
            } else {
                result = new PlanHandlerResult(UserBitShared.QueryType.PHYSICAL, planSignature);
                return;
            }
            result = new PlanHandlerResult(UserBitShared.QueryType.SQL, signature);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private Meta.Signature createPlan(Meta.Signature signature) throws RpcException, SchemaChangeException, SQLException {
        String sql = "explain plan for " + signature.sql;
        for (AvaticaParameter parameter : signature.parameters) {
            sql = sql.replaceFirst("\\?", "'" + parameter.name + "'");
        }

        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                if (rs.next()) {
                    String plan = rs.getString(2);
                    return new Meta.Signature(
                            signature.columns,
                            plan,
                            signature.parameters,
                            signature.internalParameters,
                            signature.cursorFactory
                    );
                }
                return null;
            }
        }
    }

    PlanHandlerResult getResult() {
        if (result == null) {
            throw new IllegalStateException();
        }
        return result;
    }

    static class PlanHandlerResult {
        final UserBitShared.QueryType queryType;
        final Meta.Signature signature;

        private PlanHandlerResult(UserBitShared.QueryType queryType,
                                  Meta.Signature signature) {
            this.queryType = queryType;
            this.signature = signature;
        }
    }

}
