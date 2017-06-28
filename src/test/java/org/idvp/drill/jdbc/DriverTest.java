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

import com.google.common.base.Stopwatch;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.concurrent.TimeUnit;

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

    @Test
    @Ignore
    public void test2() throws SQLException {

        long first, second, third;
        int max = 10;
        DriverManager.registerDriver(new Driver());
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        try (Connection connection = DriverManager.getConnection("jdbc:idvp:drill:drillbit=localhost:31010")) {
            try (PreparedStatement statement = connection.prepareStatement("select flatten(selector_CSS(content, '.text')) from regions.`http://bash.im/${path}` where path = ?")) {
                statement.setString(1, "best");
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        System.out.println(rs.getString(1));
                    }
                }
            }
            stopwatch.start();
            for (int i = 0; i < max; i++) {
                try (PreparedStatement statement = connection.prepareStatement("select flatten(selector_CSS(content, '.text')) from regions.`http://bash.im/${path}` where path = ?")) {
                    statement.setString(1, "abysstop");
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            System.out.println(rs.getString(1));
                        }
                    }
                }
            }
            first = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }

        try(Connection connection = DriverManager.getConnection("jdbc:drill:drillbit=localhost:31010")) {
            try (PreparedStatement statement = connection.prepareStatement("select flatten(selector_CSS(content, '.text')) from regions.`http://bash.im/${path}` where path = 'best'")) {
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        System.out.println(rs.getString(1));
                    }
                }
            }

            stopwatch.reset().start();
            for (int i = 0; i < max; i++) {
                try (PreparedStatement statement = connection.prepareStatement("select flatten(selector_CSS(content, '.text')) from regions.`http://bash.im/${path}` where path = 'abysstop'")) {
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            System.out.println(rs.getString(1));
                        }
                    }
                }
            }
            second = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            stopwatch.reset().start();
            for (int i = 0; i < max; i++) {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet rs = statement.executeQuery("select flatten(selector_CSS(content, '.text')) from regions.`http://bash.im/${path}` where path = 'abysstop'")) {
                        while (rs.next()) {
                            System.out.println(rs.getString(1));
                        }
                    }
                }
            }
            third = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        }
    }
}
