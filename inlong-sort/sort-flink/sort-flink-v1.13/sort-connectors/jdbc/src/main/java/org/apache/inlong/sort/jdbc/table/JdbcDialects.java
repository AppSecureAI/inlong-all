/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.jdbc.table;

import org.apache.inlong.sort.jdbc.dialect.MySQLDialect;
import org.apache.inlong.sort.jdbc.dialect.OracleDialect;
import org.apache.inlong.sort.jdbc.dialect.SqlServerDialect;
import org.apache.inlong.sort.jdbc.dialect.TDSQLPostgresDialect;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Default JDBC dialects.
 */
public final class JdbcDialects {

    private static final List<JdbcDialect> DIALECTS = new ArrayList<>();

    private static final Map<String, JdbcDialect> CUSTOM_DIALECTS = new LinkedHashMap<>();

    private static final Set<String> ALLOWED_DIALECT_CLASSES = new HashSet<>(Arrays.asList(
            "org.apache.inlong.sort.jdbc.dialect.ClickHouseDialect",
            "org.apache.inlong.sort.jdbc.dialect.GreenplumDialect",
            "org.apache.inlong.sort.jdbc.dialect.PostgresDialect",
            "org.apache.inlong.sort.jdbc.dialect.MySQLDialect",
            "org.apache.inlong.sort.jdbc.dialect.OracleDialect",
            "org.apache.inlong.sort.jdbc.dialect.SqlServerDialect",
            "org.apache.inlong.sort.jdbc.dialect.TDSQLPostgresDialect"));

    static {
        DIALECTS.add(new MySQLDialect());
        DIALECTS.add(new TDSQLPostgresDialect());
        DIALECTS.add(new SqlServerDialect());
        DIALECTS.add(new OracleDialect());
    }

    /**
     * Fetch the JdbcDialect class corresponding to a given database url.
     */
    public static Optional<JdbcDialect> get(String url) {
        for (JdbcDialect dialect : DIALECTS) {
            if (dialect.canHandle(url)) {
                return Optional.of(dialect);
            }
        }
        return Optional.empty();
    }

    public static Optional<JdbcDialect> getCustomDialect(String dialectImpl) {
        JdbcDialect jdbcDialect = CUSTOM_DIALECTS.get(dialectImpl);
        if (jdbcDialect != null) {
            return Optional.of(jdbcDialect);
        }
        return Optional.empty();
    }

    /**
     * Fetch the JdbcDialect class corresponding to a given database url.
     */
    public static Optional<JdbcDialect> register(String dialectImpl) {
        if (!ALLOWED_DIALECT_CLASSES.contains(dialectImpl)) {
            throw new IllegalArgumentException("Dialect class not in the allowed list: " + dialectImpl);
        }
        try {
            JdbcDialect dialect = (JdbcDialect) Class.forName(dialectImpl).newInstance();
            CUSTOM_DIALECTS.put(dialectImpl, dialect);
            return Optional.of(dialect);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot register such dialect impl: " + dialectImpl, e);
        }
    }
}
