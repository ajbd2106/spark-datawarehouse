package com.starschema.annotations.facts;

import com.starschema.lookup.AbstractLookup;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface FactJunkDimension {
    String technicalId();
}
