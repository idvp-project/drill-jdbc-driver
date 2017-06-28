package org.idvp.drill.jdbc.cache;

import org.apache.calcite.avatica.Meta;

import java.security.Signature;
import java.sql.Connection;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
public interface PlanCache {
    String getPlan(String url, String query);
    void storePlan(String url, String query, String plan);
}
