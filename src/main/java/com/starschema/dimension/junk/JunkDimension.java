package com.starschema.dimension.junk;

import com.starschema.annotations.dimensions.CheckSum;
import com.starschema.annotations.dimensions.FunctionalId;
import com.starschema.annotations.dimensions.TechnicalId;
import com.starschema.annotations.dimensions.UpdatedDate;
import com.starschema.annotations.facts.Fact;
import com.starschema.annotations.general.Table;
import com.starschema.lookup.JunkDimensionLookup;
import lombok.Data;

import java.sql.Date;

@Table(name = "junkTable", lookupType = JunkDimensionLookup.class)
@Data
public class JunkDimension implements IJunkDimension {

    @TechnicalId
    Long id;

    @Fact
    String field1;

    @UpdatedDate
    Date updatedDate;

    @CheckSum
    @FunctionalId
    String checksum;
}
