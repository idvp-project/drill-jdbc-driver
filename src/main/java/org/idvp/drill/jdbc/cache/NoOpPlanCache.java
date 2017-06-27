package org.idvp.drill.jdbc.cache;

import org.apache.calcite.avatica.Meta;

import java.sql.Connection;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
@SuppressWarnings("unused")
public class NoOpPlanCache implements PlanCache {
    @Override
    public Meta.Signature getPlan(Connection connection, String query) {
        return null;
    }

    @Override
    public void storePlan(Connection connection, String query, Meta.Signature signature) {
    }

}
