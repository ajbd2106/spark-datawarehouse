package com.starschema.lookup;

import com.starschema.annotations.dimensions.FunctionalId;
import com.starschema.annotations.dimensions.TechnicalId;
import lombok.Data;

import java.io.Serializable;

@Data
public abstract class AbstractLookup implements Serializable {

    @TechnicalId
    protected Long id;

    @FunctionalId
    protected String functionalId;

}
