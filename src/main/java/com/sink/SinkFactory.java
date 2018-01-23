package com.sink;

import com.starschema.annotations.general.Table;

public class SinkFactory {

    public static ISink createSink(SinkType sinkType, Table table){

        if(sinkType.equals(SinkType.POSTGRESQL)){
            return new PostGreSqlSink(table.name());
        }

        throw new RuntimeException("Only post gre sink is supported for the moment");
    }
}
