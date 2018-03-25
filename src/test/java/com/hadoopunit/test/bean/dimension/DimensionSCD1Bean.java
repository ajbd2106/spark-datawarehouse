package com.hadoopunit.test.bean.dimension;

import com.starschema.annotations.dimensions.*;
import com.starschema.annotations.common.Table;
import com.starschema.dimension.Dimension;
import com.starschema.lookup.PartyLookup;
import lombok.Data;

import java.sql.Date;

@Data
@Table(name = "newTable", lookupType = PartyLookup.class, stagingTable = "stagingTable", masterTable = "currentTable")
public class DimensionSCD1Bean  implements Dimension {

    @TechnicalId
    Long id;

    @FunctionalId
    String functionalId;

    @SCD1
    String name;

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
}
