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
package org.idvp.drill.jdbc;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
public class DriverTest {

    @Test
    @Ignore
    public void test1() throws SQLException {
        DriverManager.registerDriver(new Driver());
        try (Connection connection = DriverManager.getConnection("jdbc:idvp:drill:drillbit=localhost:31010")) {
            try (PreparedStatement statement = connection.prepareStatement("select flatten(selector_CSS(content, '.text')) from regions.`http://bash.im`")) {
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        System.out.println(rs.getString(1));
                    }
                }
            }
        }
    }
}
