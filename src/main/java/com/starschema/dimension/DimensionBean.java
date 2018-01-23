package com.starschema.dimension;

import com.starschema.annotations.dimensions.*;
import com.starschema.annotations.general.Table;
import com.starschema.lookup.PartyLookup;
import lombok.Data;

import java.sql.Date;


@Data
@Table(name = "newTable", lookupType = PartyLookup.class, stagingTable = "stagingTable", masterTable = "currentTable")
public class DimensionBean implements Dimension {

    @TechnicalId
    Long id;

    @FunctionalId
    String functionalId;

    @SCD1
    String name;

    @SCD2
    String type;

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
