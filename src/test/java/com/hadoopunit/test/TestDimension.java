package com.hadoopunit.test;

import com.sink.ISink;
import com.sink.SinkFactory;
import com.sink.SinkType;
import com.sink.postgres.InsertMode;
import com.source.ISource;
import com.source.SourceFactory;
import com.spark.SparkSessionFactory;
import com.starschema.Processor;
import com.starschema.ProcessorFactory;
import com.starschema.annotations.common.Table;
import com.starschema.columnSelector.AnnotatedDimensionColumnSelector;
import com.starschema.columnSelector.DimensionColumnSelector;
import com.starschema.dimension.DimensionBean;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.Serializable;
import java.time.LocalDate;

public class TestDimension implements Serializable {

    @Test
    public void sparkScd2() {
        SparkSession sparkSession = SparkSessionFactory.createClassicSparkSession("local[*]", "Java Spark SQL basic example");

        Table tableInfo = DimensionBean.class.getAnnotation(Table.class);

        ISource stagingSource = SourceFactory.createPostGreSqlSource(DimensionBean.class.getAnnotation(Table.class).stagingTable());
        ISource masterTableSource = SourceFactory.createPostGreSqlSource(tableInfo.masterTable());
        ISink dimensionSink = SinkFactory.createSink(SinkType.POSTGRESQL, tableInfo, true, InsertMode.OVERWRITE);
        ISink lookupSink = SinkFactory.createSink(SinkType.POSTGRESQL, tableInfo.lookupType().getAnnotation(Table.class), InsertMode.OVERWRITE);
        DimensionColumnSelector dimensionColumnSelector = new AnnotatedDimensionColumnSelector<>(DimensionBean.class);

        Processor<DimensionBean> dimensionProcessor = ProcessorFactory.createDimensionProcessor(
                sparkSession,
                DimensionBean.class,
                masterTableSource,
                stagingSource,
                dimensionSink,
                lookupSink,
                LocalDate.now(),
                dimensionColumnSelector);

        dimensionProcessor.process();
    }
}
