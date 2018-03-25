package com.hadoopunit.test.sources;

import com.hadoopunit.test.sink.ListSink;
import com.sink.ISink;
import com.source.IFactSourceFactory;
import com.source.ISource;
import com.starschema.annotations.common.Table;
import com.starschema.dimension.junk.IJunkDimension;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

@Getter
public class MockFactSourceFactory implements IFactSourceFactory {

    Map<String, ISource> sourceMap = new HashMap<>();
    Map<String, ListSink> sinkMap = new HashMap<>();

    @Override
    public ISource getJunkDimensionSource(Class<? extends IJunkDimension> junkDimension) {
        return sourceMap.get(junkDimension.getAnnotation(Table.class).lookupType().getSimpleName());
    }

    @Override
    public ISource getLookupSource(Class<?> targetClass) {
        return sourceMap.get(targetClass.getSimpleName());
    }


    @Override
    public ISink getJunkDimensionSink(Class<? extends IJunkDimension> junkDimension) {
        String sinkName = junkDimension.getSimpleName();
        sinkMap.put(sinkName, new ListSink());
        return sinkMap.get(sinkName);

    }

    @Override
    public ISink getJunkDimensionLookupSink(Class<? extends IJunkDimension> junkDimension) {
        String sinkName = junkDimension.getAnnotation(Table.class).lookupType().getSimpleName();
        sinkMap.put(sinkName, new ListSink());
        return sinkMap.get(sinkName);

    }

    public void clear(){
        sourceMap.clear();
        sinkMap.clear();
    }

}
