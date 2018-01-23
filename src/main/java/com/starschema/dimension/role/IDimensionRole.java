package com.starschema.dimension.role;

import com.starschema.annotations.dimensions.TechnicalId;
import com.starschema.lookup.AbstractLookup;
import com.utils.ReflectUtils;
import org.apache.spark.sql.Column;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public interface IDimensionRole {
    Class<?> getFactLookupClass(boolean getFromMasterTable);

    default Class<?> getFactLookupClass(){
        return getFactLookupClass(false);
    };

    String getAlias();

    Column getFunctionalId(String alias);

    String getTechnicalIdFieldName();

    Class<?> getMasterTableClass();

    List<Column> getMasterTableFlattenedFields();
}
