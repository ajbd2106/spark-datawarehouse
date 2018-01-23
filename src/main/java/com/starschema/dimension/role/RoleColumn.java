package com.starschema.dimension.role;

import lombok.Getter;

@Getter
public class RoleColumn {

    private final String fieldName;
    private final String roleName;
    private final Object defaultValue;

    public RoleColumn(String fieldName, String roleName, Object defaultValue) {
        this.fieldName = fieldName;
        this.roleName = roleName;
        this.defaultValue = defaultValue;
    }

}
