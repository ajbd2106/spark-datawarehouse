package com.starschema.dimension.role;

import com.starschema.annotations.dimensions.TechnicalId;
import com.utils.ReflectUtils;
import org.apache.spark.sql.Column;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public abstract class AbstractDimensionRole implements IDimensionRole {

    protected final Field field;

    public AbstractDimensionRole(Field field) {
        this.field = field;
    }

    /***
     * @return get an alias to join the table that corresponds to field name
     */
    @Override
    public String getAlias() {
        return field.getName();
    }

    protected abstract List<Class<? extends Annotation>> getMasterTableFlattenedAnnotations();

    protected abstract List<RoleColumn> getDimensionCaseColumns(List<Field> fields);

    @Override
    public List<Column> getMasterTableFlattenedFields() {
        String alias = getAlias();
        Class<?> dimension = getMasterTableClass();

        String technicalIdName = String.format("%s.%s", alias, ReflectUtils.getUniqueField(TechnicalId.class, dimension).get().getName());

        List<Field> fields = ReflectUtils.getFields(getMasterTableFlattenedAnnotations(), dimension);

        List<RoleColumn> roleColumns = getDimensionCaseColumns(fields);

        return roleColumns.stream().map(
                roleColumn -> {
                    String finalAlias = String.format("%s_%s", roleColumn.getRoleName(), roleColumn.getFieldName());
                    return when(col(technicalIdName).isNull(), lit(roleColumn.getDefaultValue()))
                            .otherwise(col(String.format("%s.%s", alias, roleColumn.getFieldName()))).as(finalAlias);
                }
        ).collect(Collectors.toList());
    }
}
