/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.jdbc.impl;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
public class EnhancedDriverImpl extends DriverImpl {
    private final static String PREFIX = "jdbc:idvp:drill:";

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
        return new EnhancedDrillMeta((DrillConnectionImpl)connection);
    }
}
