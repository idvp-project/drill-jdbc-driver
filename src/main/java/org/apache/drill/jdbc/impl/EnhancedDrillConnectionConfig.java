package org.apache.drill.jdbc.impl;

import org.apache.drill.jdbc.DrillConnectionConfig;
import org.idvp.drill.jdbc.cache.ConcurrentMapPlanCache;
import org.idvp.drill.jdbc.integration.DefaultIntegration;

import java.util.Properties;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
class EnhancedDrillConnectionConfig extends DrillConnectionConfig {
    EnhancedDrillConnectionConfig(Properties p) {
        super(p);
    }

    boolean enabled() {
        return "true".equalsIgnoreCase(properties.getProperty(Constants.PLANNING_ENABLED, "true"));
    }

    String getCacheClassName() {
        return properties.getProperty(Constants.PLAN_CACHE_IMPL, ConcurrentMapPlanCache.class.getName());
    }

    String getIntegration() {
        return properties.getProperty(Constants.PLAN_INTEGRATION);
    }
}
