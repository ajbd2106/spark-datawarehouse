package com.hadoopunit.test.bean.dimension;

import com.starschema.annotations.dimensions.*;
import com.starschema.annotations.general.Table;
import com.starschema.dimension.Dimension;
import com.starschema.lookup.PartyLookup;
import lombok.Data;

import java.math.BigDecimal;
import java.sql.Date;

@Data
@Table(name = "newTable", lookupType = PartyLookup.class, stagingTable = "stagingTable", masterTable = "currentTable")
public class DimensionAllTypesFieldsBean implements Dimension {

    @TechnicalId
    Long id;

    @FunctionalId
    String functionalId;

    @SCD1
    String scd1StringField;

    @SCD1(defaultValue = "0")
    Integer scd1IntegerField;

    @SCD1(defaultValue = "0")
    Long scd1LongField;

    @SCD1(defaultValue = "0.00")
    BigDecimal scd1BigDecimalField;

    @SCD1(defaultValue = "1970-01-01")
    Date scd1DateField;

    @SCD2(defaultValue = "N/A")
    String scd2StringField;

    @SCD2(defaultValue = "-1")
    Integer scd2IntegerField;

    @SCD2(defaultValue = "-1")
    Long scd2LongField;

    @SCD2(defaultValue = "-1")
    BigDecimal scd2BigDecimalField;

    @SCD2(defaultValue = "1970-01-01")
    Date scd2DateField;

    @StartDate
    Date startDate;

    @EndDate
    Date endDate;

    @UpdatedDate
    Date updatedDate;

    @Current
    String current;

    @SCD1CheckSum
    String scd1Checksum;

    @SCD2CheckSum
    String scd2Checksum;
}
