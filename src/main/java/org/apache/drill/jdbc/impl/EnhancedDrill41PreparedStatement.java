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

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.drill.exec.proto.UserProtos;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.List;

/**
 * @author Oleg Zinoviev
 * @since 26.06.2017.
 */
public class EnhancedDrill41PreparedStatement extends DrillPreparedStatementImpl {
    private final Meta.Signature signature;

    EnhancedDrill41PreparedStatement(DrillConnectionImpl connection,
                                     Meta.StatementHandle h,
                                     Meta.Signature signature,
                                     UserProtos.PreparedStatement protoPreparedStatement,
                                     int resultSetType,
                                     int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        super(connection, h, signature, protoPreparedStatement, resultSetType, resultSetConcurrency, resultSetHoldability);
        this.signature = signature;
    }

    @Override
    public List<TypedValue> getParameterValues() {
        return super.getParameterValues();
    }

    @Override
    protected AvaticaParameter getParameter(int param) throws SQLException {
        try {
            return this.signature.parameters.get(param - 1);
        } catch (IndexOutOfBoundsException var3) {
            throw this.connection.helper.toSQLException(this.connection.helper.createException("parameter ordinal " + param + " out of range"));
        }
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.getSite(parameterIndex).setRowId(x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        this.getSite(parameterIndex).setNString(value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        this.getSite(parameterIndex).setNCharacterStream(value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.getSite(parameterIndex).setNClob(value);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.getSite(parameterIndex).setClob(reader, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        this.getSite(parameterIndex).setBlob(inputStream, length);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.getSite(parameterIndex).setNClob(reader, length);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.getSite(parameterIndex).setSQLXML(xmlObject);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.getSite(parameterIndex).setAsciiStream(x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.getSite(parameterIndex).setBinaryStream(x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.getSite(parameterIndex).setCharacterStream(reader, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.getSite(parameterIndex).setAsciiStream(x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.getSite(parameterIndex).setBinaryStream(x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.getSite(parameterIndex).setCharacterStream(reader);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        this.getSite(parameterIndex).setNCharacterStream(value);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.getSite(parameterIndex).setClob(reader);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.getSite(parameterIndex).setBlob(inputStream);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.getSite(parameterIndex).setNClob(reader);
    }
}
