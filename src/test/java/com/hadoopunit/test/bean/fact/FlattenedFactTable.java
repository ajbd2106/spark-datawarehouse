package com.hadoopunit.test.bean.fact;

import com.starschema.fact.IFact;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class FlattenedFactTable implements IFact {

    Integer inventoryDate;

    Long primaryBorrower_id;
    String primaryBorrower_functionalId;
    String primaryBorrower_name;
    String primaryBorrower_type;

    Long obligor_id;
    String obligor_functionalId;
    String obligor_name;
    String obligor_type;

    BigDecimal amount1;
    BigDecimal amount2;

    Long junkDimension1_id;
    String junkDimension1_field1;
    Long junkDimension2_id;
    String junkDimension2_field1;
}
