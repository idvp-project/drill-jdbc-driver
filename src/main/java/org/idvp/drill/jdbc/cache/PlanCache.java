package org.idvp.drill.jdbc.cache;

import org.apache.calcite.avatica.Meta;

import java.security.Signature;
import java.sql.Connection;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
public interface PlanCache {
    Meta.Signature getPlan(Connection connection, String query);
    void storePlan(Connection connection, String query, Meta.Signature signature);
}
