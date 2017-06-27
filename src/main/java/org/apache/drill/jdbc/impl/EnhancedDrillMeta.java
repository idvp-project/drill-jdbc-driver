package org.apache.drill.jdbc.impl;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.drill.exec.planner.sql.DrillParserConfig;
import org.apache.drill.jdbc.impl.avatica.SelectSignature;

import java.sql.JDBCType;
import java.util.*;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
public class EnhancedDrillMeta extends DrillMetaImpl {
    EnhancedDrillMeta(DrillConnectionImpl connection) {
        super(connection);
    }
    
    

    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        StatementHandle result = super.createStatement(ch);
        result.signature = createSignature(sql);
        return result;
    }

    private Signature createSignature(String sql) {
        try {
            SqlNode query = SqlParser.create(sql, new EnhancedDrillParserConfig())
                    .parseQuery();

            List<AvaticaParameter> parameters = new ArrayList<>();

            query.accept(new SqlBasicVisitor<List<AvaticaParameter>>() {
                @Override
                public List<AvaticaParameter> visit(SqlDynamicParam param) {
                    AvaticaParameter parameter = new AvaticaParameter(
                            false,
                            0,
                            0,
                            JDBCType.JAVA_OBJECT.ordinal(),
                            JDBCType.JAVA_OBJECT.getName(),
                            Object.class.getName(),
                            "?" + param.getIndex()
                    );
                    parameters.add(parameter);
                    super.visit(param);
                    return parameters;
                }
            });



            return new SelectSignature(new DrillColumnMetaDataList(), sql, parameters, Collections.emptyMap(), null);

        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }
}
