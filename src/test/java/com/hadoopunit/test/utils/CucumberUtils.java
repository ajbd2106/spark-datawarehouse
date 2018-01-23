package com.hadoopunit.test.utils;

import com.hadoopunit.test.comparator.ReflectComparator;
import cucumber.api.DataTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class CucumberUtils {

    private static final String TAG_CURRENT_DATE = "CURRENTDATE";
    private static final int BIGDECIMAL_SCALE = 6;
    private static final RoundingMode BIGDECIMAL_ROUNDINGMODE = RoundingMode.HALF_UP;

    private static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public static <T> List<T> convertRowDatasetToObject(DataTable expectedLines, List<Row> dataTable, Class<T> className) {

        String[] fieldNames = getFieldNames(expectedLines, dataTable);

        return dataTable.
                stream()
                .map(row -> {
                    try {
                        T newInstance = className.newInstance();

                        for (String fieldName : fieldNames) {
                            int index = row.fieldIndex(fieldName);
                            Object value = row.get(index);
                            Field field = FieldUtils.getField(className, fieldName, true);
                            try {
                                if (value instanceof BigDecimal) {
                                    value = ((BigDecimal) value).setScale(BIGDECIMAL_SCALE, BIGDECIMAL_ROUNDINGMODE);
                                }
                                FieldUtils.writeField(field, newInstance, value, true);
                            } catch (IllegalAccessException e) {
                                String errorMessage = String.format("Could not write field %s of class %s", field.getName(), newInstance.getClass().getCanonicalName());
                                Assert.assertTrue(errorMessage, false);
                            }
                        }

                        return newInstance;

                    } catch (InstantiationException | IllegalAccessException e) {
                        Assert.assertTrue("Error while instantiating object", false);
                        return null;
                    }
                }).collect(Collectors.toList());
    }

    private static String[] getFieldNames(DataTable expectedLines, List<Row> dataTable) {
        String[] fieldNames = dataTable.get(0).schema().fieldNames();
        if (expectedLines != null) {
            Collection<String> fieldNamesList = getDatatableHeader(expectedLines).values();
            fieldNames = fieldNamesList.toArray(new String[fieldNamesList.size()]);
        }
        return fieldNames;
    }

    public static <T> List<T> convertDataTableToObject(DataTable dataTable, Class<T> className) {

        Map<Integer, String> indexedColumns = getDatatableHeader(dataTable);

        return dataTable.getGherkinRows()
                .stream().skip(1)
                .map(row -> {
                    try {
                        T newInstance = className.newInstance();

                        indexedColumns.entrySet().forEach(
                                entry -> {
                                    String value = row.getCells().get(entry.getKey());
                                    String fieldName = entry.getValue();
                                    String[] splittedFieldName = StringUtils.split(fieldName, '.');

                                    if (splittedFieldName.length == 1) {
                                        Field field = FieldUtils.getField(className, fieldName, true);
                                        if(field == null){
                                            log.error(String.format("cannot get field named %s from class %s", fieldName, className.getCanonicalName()));
                                        }

                                        writeField(newInstance, value, field);
                                    } else if (splittedFieldName.length == 2) {
                                        String parentFieldName = splittedFieldName[0];
                                        String childFieldName = splittedFieldName[1];

                                        Field parentField = FieldUtils.getField(className, parentFieldName, true);
                                        if(parentField == null) {
                                            log.error(String.format("cannot get field named %s from class %s", parentFieldName, className.getCanonicalName()));
                                        }
                                        try {
                                            Object parentBean = FieldUtils.readField(parentField, newInstance, true);
                                            if (parentBean == null) {
                                                FieldUtils.writeField(parentField, newInstance, parentField.getType().newInstance(), true);
                                                parentBean = FieldUtils.readField(parentField, newInstance, true);
                                            }

                                            Field childField = FieldUtils.getField(parentField.getType(), childFieldName, true);
                                            if(childField == null) {
                                                log.error(String.format("cannot get field named %s from class %s", parentFieldName, className.getCanonicalName()));
                                            }
                                            writeField(parentBean, value, childField);

                                        } catch (IllegalAccessException e) {
                                            Assert.assertTrue(String.format("Could not read field %s belonging to type %s", splittedFieldName[0], className.getCanonicalName()), false);
                                        } catch (InstantiationException e) {
                                            Assert.assertTrue(String.format("Could not instantiate object of type %s", parentField.getType().getCanonicalName()), false);
                                        }


                                    }
                                }
                        );

                        return newInstance;

                    } catch (InstantiationException | IllegalAccessException e) {
                        Assert.assertTrue("Error while instantiating object", false);
                        return null;
                    }
                }).collect(Collectors.toList());
    }

    public static Map<Integer, String> getDatatableHeader(DataTable dataTable) {
        List<String> columns = dataTable.getGherkinRows().get(0).getCells();
        return columns.stream().collect(Collectors.toMap(column -> columns.indexOf(column), column -> column));
    }

    private static void writeField(Object newInstance, String value, Field field) {
        String errorMessage = String.format("Could not write field %s of class %s", field.getName(), newInstance.getClass().getCanonicalName());

        try {
            if (String.class.isAssignableFrom(field.getType())) {
                FieldUtils.writeField(field, newInstance, value, true);
            } else if (Long.class.isAssignableFrom(field.getType())) {
                FieldUtils.writeField(field, newInstance, Long.valueOf(value), true);
            } else if (BigDecimal.class.isAssignableFrom(field.getType())) {
                FieldUtils.writeField(field, newInstance, new BigDecimal(value).setScale(BIGDECIMAL_SCALE, BIGDECIMAL_ROUNDINGMODE), true);
            } else if (Date.class.isAssignableFrom(field.getType())) {
                Date date = Date.valueOf(LocalDate.now());
                if(! value.equals(TAG_CURRENT_DATE)){
                    date = new Date(dateFormatter.parse(value).getTime());
                }

                FieldUtils.writeField(field, newInstance, date, true);
            } else if (Integer.class.isAssignableFrom(field.getType())) {
                FieldUtils.writeField(field, newInstance, Integer.valueOf(value), true);
            }

        } catch (IllegalAccessException e) {
            Assert.assertTrue(errorMessage, false);
        } catch (ParseException e) {
            Assert.assertTrue("wrong date format", false);
        }
    }

    public static <T> ReflectComparator<T> createReflectComparatorFromHeader(DataTable dataTable, Class<T> targetClass) {
        return new ReflectComparator(CucumberUtils.getDatatableHeader(dataTable).values(), targetClass);
    }

    public static <T> void compareDataTableAndRow(DataTable expectedLines, List<Row> rowDataset, Class<T> targetClass) {
        ReflectComparator<T> reflectComparator = CucumberUtils.createReflectComparatorFromHeader(expectedLines, targetClass);
        List<T> expectedLinesList = CucumberUtils.convertDataTableToObject(expectedLines, targetClass);
        expectedLinesList.sort(reflectComparator);
        List<T> factTableData = CucumberUtils.convertRowDatasetToObject(expectedLines, rowDataset, targetClass);
        factTableData.sort(reflectComparator);

        Assert.assertTrue("expected and current fact table are not equal", ListUtils.isEqualList(expectedLinesList, factTableData));
    }

}
