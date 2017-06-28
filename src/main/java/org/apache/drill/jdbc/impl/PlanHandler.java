package org.apache.drill.jdbc.impl;

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
    private final EnhancedDrillConnectionImpl connection;
    private final String sql;
    private PlanHandlerResult result;

    PlanHandler(EnhancedDrillConnectionImpl connection,
                String sql) {

        this.connection = connection;
        this.sql = sql;
    }


    void handle() throws SQLException {
        try {

            EnhancedDrillConnectionConfig config = connection.getEnhancedConfig();
            if (config.enabled()) {
                String plan = connection.getCache().getPlan(connection.url, sql);
                if (plan == null) {
                    plan = createPlan(sql);
                    connection.getCache().storePlan(connection.url, sql, plan);
                }
                if (plan != null) {
                    result = new PlanHandlerResult(UserBitShared.QueryType.PHYSICAL, plan);
                    return;
                }
            }
            result = new PlanHandlerResult(UserBitShared.QueryType.SQL, sql);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private String createPlan(String sql) throws RpcException, SchemaChangeException, SQLException {
        sql = "explain plan for " + sql;
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                if (rs.next()) {
                    return rs.getString(2);
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
        final String plan;

        private PlanHandlerResult(UserBitShared.QueryType queryType,
                                  String plan) {
            this.queryType = queryType;
            this.plan = plan;
        }
    }

}
