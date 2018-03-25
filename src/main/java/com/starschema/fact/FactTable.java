package com.starschema.fact;

import com.starschema.annotations.facts.Fact;
import com.starschema.annotations.facts.FactDimension;
import com.starschema.annotations.facts.FactJunkDimension;
import com.starschema.annotations.facts.InventoryDate;
import com.starschema.annotations.common.Table;
import com.starschema.dimension.DimensionBean;
import com.starschema.dimension.junk.JunkDimension;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Table(name ="factTable")
public class FactTable implements IFact {

    @InventoryDate
    Integer inventoryDate;

    Long partyRole1Id;

    Long partyRole2Id;

    Long junkDimension1Id;

    Long junkDimension2Id;

    @FactDimension(lookupType = DimensionBean.class, technicalId = "partyRole1Id", roleName = "primaryBorrower")
    String partyRole1FuncId;

    @FactDimension(lookupType = DimensionBean.class, technicalId = "partyRole2Id", roleName = "obligor")
    String partyRole2FuncId;

    @Fact
    BigDecimal amount1;

    @Fact
    BigDecimal amount2;

    @FactJunkDimension(technicalId = "junkDimension1Id")
    JunkDimension junkDimension1;

    @FactJunkDimension(technicalId = "junkDimension2Id")
    JunkDimension junkDimension2;

}
