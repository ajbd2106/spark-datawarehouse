package com.starschema;

public enum Alias {
    STAGE, CURRENT;

    public String getLabel(){
        return this.toString().toLowerCase();
    }
}
