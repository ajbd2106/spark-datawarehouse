package com.sink;

import com.sink.postgres.InsertMode;
import com.sink.postgres.PostGreSqlSink;
import com.starschema.annotations.common.Table;

public class SinkFactory {

    public static ISink createSink(SinkType sinkType, Table table) {
        return createSink(sinkType, table, false, InsertMode.APPEND);
    }

    public static ISink createSink(SinkType sinkType, Table table, InsertMode insertMode) {
        return createSink(sinkType, table, false, insertMode);
    }

    public static ISink createSink(SinkType sinkType, Table table, boolean cloneMasterTable, InsertMode insertMode) {

        if (sinkType.equals(SinkType.POSTGRESQL)) {
            if (cloneMasterTable) {
                return new PostGreSqlSink(table.name(), table.masterTable(), insertMode);
            } else {
                return new PostGreSqlSink(table.name(), insertMode);
            }

        }

        throw new RuntimeException("Only post gre sink is supported for the moment");
    }
}
