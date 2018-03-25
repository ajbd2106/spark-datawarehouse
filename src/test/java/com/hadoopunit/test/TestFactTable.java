package com.hadoopunit.test;

import com.hadoopunit.test.sources.BeanSourceMock;
import com.sink.ISink;
import com.sink.SinkFactory;
import com.sink.SinkType;
import com.source.IFactSourceFactory;
import com.source.PostGreSqlFactSourceFactory;
import com.spark.SparkSessionFactory;
import com.starschema.Processor;
import com.starschema.ProcessorFactory;
import com.starschema.annotations.common.Table;
import com.starschema.columnSelector.AnnotatedFactColumnSelector;
import com.starschema.dimension.junk.JunkDimension;
import com.starschema.fact.FactTable;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class TestFactTable {

    @Test
    public void sparkFactTable() {

        List<FactTable> factTable = new ArrayList<>();

        factTable.add(createFactData("3543", "3543", BigDecimal.valueOf(4586123.4512), BigDecimal.valueOf(4512345.845), "field1Value", "field3Value"));
        factTable.add(createFactData("3542", "3544", BigDecimal.valueOf(7845123.4512), BigDecimal.valueOf(6547864.845), "field2Value", "field2Value"));
        factTable.add(createFactData("3541", "3545", BigDecimal.valueOf(9845641.4512), BigDecimal.valueOf(7845312.845), "field1Value", "field1Value"));

        BeanSourceMock<FactTable> beanSourceMock = new BeanSourceMock(FactTable.class, factTable);
        SparkSession sparkSession = SparkSessionFactory.createClassicSparkSession("local[*]", "Java Spark SQL basic example");

        ISink factSink = SinkFactory.createSink(SinkType.POSTGRESQL, FactTable.class.getAnnotation(Table.class));
        IFactSourceFactory factSourceFactory = new PostGreSqlFactSourceFactory();

        Processor<FactTable> factProcessor = ProcessorFactory.createFactProcessor(
                sparkSession, FactTable.class, LocalDate.now(), beanSourceMock, factSourceFactory, factSink, new AnnotatedFactColumnSelector(FactTable.class)
        );
        factProcessor.process();

//        Processor<FactTable> flattenedFactProcessor = ProcessorFactory.createFlattenedFactProcessor(sparkSession, FactTable.class, LocalDate.now(), beanSourceMock, factSourceFactory, factSink,
//                new AnnotatedFactColumnSelector(FactTable.class));
//        flattenedFactProcessor.process();
    }

    private FactTable createFactData(String partyRole1FuncId, String partyRole2FuncId, BigDecimal amount1, BigDecimal amount2, String junkDimension1Field1, String junkDimension2Field1) {
        FactTable factTable = new FactTable();
        factTable.setPartyRole1FuncId(partyRole1FuncId);
        factTable.setPartyRole2FuncId(partyRole2FuncId);
        factTable.setAmount1(amount1);
        factTable.setAmount2(amount2);

        JunkDimension junkDimension1 = new JunkDimension();
        junkDimension1.setField1(junkDimension1Field1);
        factTable.setJunkDimension1(junkDimension1);

        JunkDimension junkDimension2 = new JunkDimension();
        junkDimension2.setField1(junkDimension2Field1);
        factTable.setJunkDimension2(junkDimension2);
        return factTable;
    }


}

