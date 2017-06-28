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

    private final static ConcurrentMap<String, String> cache = new ConcurrentHashMap<>();

    @Override
    public String getPlan(String url, String query) {
        //noinspection StringBufferReplaceableByString
        return cache.get(new StringBuilder()
            .append(url)
            .append(":")
            .append(query)
            .toString());

    }

    @Override
    public void storePlan(String url, String query, String plan) {
        //noinspection StringBufferReplaceableByString
        cache.put(new StringBuilder()
                .append(url)
                .append(":")
                .append(query)
                .toString(), plan);
    }
}
