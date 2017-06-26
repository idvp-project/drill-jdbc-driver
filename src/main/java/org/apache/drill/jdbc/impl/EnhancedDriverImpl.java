package org.idvp.drill.jdbc.impl;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.drill.jdbc.impl.DrillConnectionImpl;
import org.apache.drill.jdbc.impl.DrillMetaImpl;
import org.apache.drill.jdbc.impl.EnhancedDrillJdbc40Factory;
import org.apache.drill.jdbc.impl.EnhancedDrillJdbc41Factory;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
public class DriverImpl extends UnregisteredDriver {
    private final static String PREFIX = "jdbc:idvp:drill:";
    private static final String METADATA_PROPERTIES_RESOURCE_PATH = "apache-drill-jdbc.properties";

    @Override
    protected DriverVersion createDriverVersion() {
        return DriverVersion.load(this.getClass(), METADATA_PROPERTIES_RESOURCE_PATH,
                "iDVP Apache Drill JDBC Driver",
                DrillVersionInfo.getVersion(),
                "Apache Drill",
                DrillVersionInfo.getVersion());
    }

    @Override
    protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        switch (jdbcVersion) {
            case JDBC_30:
                throw new IllegalStateException("JDBC 3.0 is not supported");
            case JDBC_40:
                return EnhancedDrillJdbc40Factory.class.getName();
            case JDBC_UNKNOWN:
            case JDBC_41:
            default:
                return EnhancedDrillJdbc41Factory.class.getName();
        }
    }

    @Override
    protected String getConnectStringPrefix() {
        return PREFIX;
    }

    @Override
    public Meta createMeta(AvaticaConnection connection) {
        return new org.apache.drill.jdbc.impl.DrillMetaImpl((DrillConnectionImpl) connection);
    }
}
