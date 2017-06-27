package org.apache.drill.jdbc.impl;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.drill.exec.planner.sql.parser.impl.DrillParserWithCompoundIdConverter;

public class EnhancedDrillParserConfig implements SqlParser.Config {
    public int identifierMaxLength() {
        return Integer.MAX_VALUE;
    }

    public Casing quotedCasing() {
        return Casing.UNCHANGED;
    }

    public Casing unquotedCasing() {
        return Casing.UNCHANGED;
    }

    public Quoting quoting() {
        return Quoting.BACK_TICK;
    }

    public boolean caseSensitive() {
        return false;
    }

    public SqlParserImplFactory parserFactory() {
        return DrillParserWithCompoundIdConverter.FACTORY;
    }
}