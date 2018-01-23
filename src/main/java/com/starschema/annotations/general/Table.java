package com.starschema.annotations.general;

import com.starschema.lookup.AbstractLookup;
import org.apache.commons.lang3.StringUtils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Table {
    String name();
    Class<? extends AbstractLookup> lookupType() default AbstractLookup.class;

    String stagingTable() default StringUtils.EMPTY;
    String masterTable() default StringUtils.EMPTY;

}


