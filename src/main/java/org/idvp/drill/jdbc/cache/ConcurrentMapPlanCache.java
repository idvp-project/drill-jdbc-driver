package org.idvp.drill.jdbc.cache;

import org.apache.calcite.avatica.Meta;

import java.sql.Connection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
@SuppressWarnings("unused")
public class ConcurrentMapPlanCache implements PlanCache {

    private final static ConcurrentMap<Connection, ConcurrentMap<String, Meta.Signature>> cache = new ConcurrentHashMap<>();

    @Override
    public Meta.Signature getPlan(Connection connection, String query) {
        return cache
                .computeIfAbsent(connection, c -> new ConcurrentHashMap<>())
                .get(query);
    }

    @Override
    public void storePlan(Connection connection, String query, Meta.Signature signature) {
        cache
                .computeIfAbsent(connection, c -> new ConcurrentHashMap<>())
                .put(query, signature);
    }
}
