package org.apache.drill.jdbc.impl;

import org.apache.calcite.avatica.AvaticaFactory;
import org.idvp.drill.jdbc.cache.PlanCache;
import org.idvp.drill.jdbc.integration.DefaultIntegration;
import org.idvp.drill.jdbc.integration.Integration;

import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
class EnhancedDrillConnectionImpl extends DrillConnectionImpl {
    private PlanCache cache;
    private Integration integration;

    EnhancedDrillConnectionImpl(DriverImpl driver,
                                AvaticaFactory factory,
                                String url,
                                Properties info) throws SQLException {
        super(driver, factory, url, info);
        createPlanCache();
    }

    private void createPlanCache() {
        EnhancedDrillConnectionConfig config = getEnhancedConfig();
        integration = new DefaultIntegration();
        if (config.getIntegration() != null) {
            integration = (Integration) integration.instance(config.getIntegration());
        }

        this.cache = (PlanCache) integration.instance(config.getCacheClassName());
    }

    EnhancedDrillConnectionConfig getEnhancedConfig() {
        return new EnhancedDrillConnectionConfig(info);
    }

    PlanCache getCache() {
        return cache;
    }

    Integration getIntegration() {
        return integration;
    }
}
