package com.starschema;

@FunctionalInterface
public interface Processor<T> {
    void process();
}
