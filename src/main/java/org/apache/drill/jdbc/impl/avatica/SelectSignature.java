package org.apache.drill.jdbc.impl.avatica;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import java.util.List;
import java.util.Map;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
public final class SelectSignature extends Meta.Signature {
    public SelectSignature(List<ColumnMetaData> columns,
                           String sql,
                           List<AvaticaParameter> parameters,
                           Map<String, Object> internalParameters,
                           Meta.CursorFactory cursorFactory) {
        super(columns, sql, parameters, internalParameters, cursorFactory);
    }
}
