package com.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.function.BinaryOperator;

import static org.apache.spark.sql.functions.max;

public class SparkFunctionUtils {

    /***
     *
     * @param offset an offset to start the sequence
     * @param newLines a dataset of new lines, which have some fictive technical id as first column of the row.
     * @return return a JavaRDD<Row> with technical id generate as a continuous sequence.
     * technical id will replace the first column of the row
     */
    public static Dataset<Row> generateTechnicalId(SparkSession sparkSession, Long offset, Dataset<Row> newLines) {
        StructType schema = newLines.schema();

        JavaRDD<Row> newLinesWithTechnicalId = newLines
                .toJavaRDD()
                .zipWithIndex()
                .map(tuple -> {
                    Row currentRow = tuple._1;
                    Long technicalId = tuple._2;
                    Object[] object = new Object[currentRow.length()];
                    object[0] = technicalId + offset + 1;
                    //don't take first field which should be a fake technical id
                    for (int i = 1; i < tuple._1.length(); i++) {
                        object[i] = tuple._1.get(i);
                    }
                    return RowFactory.create(object);
                });

        return sparkSession.createDataFrame(newLinesWithTechnicalId, schema);
    }

    public static Dataset<Row> addTechnicalId(SparkSession sparkSession, Long offset, Dataset<Row> newLines) {
        StructType schema = newLines.schema();

        StructType newSchema = new StructType();
        newSchema = newSchema.add("technicalId", DataTypes.LongType);

        for(StructField structField : schema.fields()){
            newSchema = newSchema.add(structField.name(), structField.dataType());
        }

        JavaRDD<Row> newLinesWithTechnicalId = newLines
                .toJavaRDD()
                .zipWithIndex()
                .map(tuple -> {
                    Row currentRow = tuple._1;
                    Long technicalId = tuple._2;
                    Object[] object = new Object[currentRow.length() +1];
                    object[0] = technicalId + offset + 1;
                    //don't take first field which should be a fake technical id
                    for (int i = 0; i < tuple._1.length(); i++) {
                        object[i +1] = tuple._1.get(i);
                    }
                    return RowFactory.create(object);
                });



        return sparkSession.createDataFrame(newLinesWithTechnicalId, newSchema);
    }

    public static Long getMaxRowId(Dataset dataset, String fieldName){
        List<Row> maxIdRow = dataset.select(max(fieldName)).collectAsList();
        Long maxId = 0l;
        if (maxIdRow.get(0).get(0) != null) {
            maxId = maxIdRow.get(0).getLong(0);
        }

        return maxId;
    }
}
