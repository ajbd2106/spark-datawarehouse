package com.starschema.columnSelector;

import com.starschema.dimension.role.IDimensionRole;
import org.apache.spark.sql.Column;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public interface FactColumnSelector {
    List<IDimensionRole> getLookupDimension();

    Map<Class, List<Field>> getJunkDimensions();

    String getLookupFunctionalId(Class<?> lookup, String alias);

    String getInventoryDateName();

    Column[] getFinalColumns(Integer inventoryDate, String inventoryDateFieldName, List<IDimensionRole> factDimensionRoleList, String currentAlias);

    Column[] getFlattenedDimensionColumns(List<IDimensionRole> factDimensionRoleList, Integer inventoryDate, String inventoryDateFieldName, String currentAlias);

    Column[] removeAliasFromColumns(String alias, Class targetClass);

    Column inventoryDateBetweenStartAndEndDate(LocalDate inventoryDate, Class lookupType);

    Column[] getLookupTableColumns(Class lookupType);
}
