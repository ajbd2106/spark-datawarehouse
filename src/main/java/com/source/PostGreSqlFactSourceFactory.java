package com.source;

import com.sink.ISink;
import com.sink.SinkFactory;
import com.sink.SinkType;
import com.starschema.annotations.general.Table;
import com.starschema.dimension.Dimension;
import com.starschema.dimension.junk.IJunkDimension;
import com.starschema.lookup.AbstractLookup;

public class PostGreSqlFactSourceFactory implements IFactSourceFactory {

    @Override
    public ISource getJunkDimensionSource(Class<? extends IJunkDimension> junkDimension) {
        return SourceFactory.createPostGreSqlSource(junkDimension.getAnnotation(Table.class).lookupType().getAnnotation(Table.class).name());
    }

    @Override
    public ISource getLookupSource(Class<?> targetClass) {
        if (IJunkDimension.class.isAssignableFrom(targetClass)
                || AbstractLookup.class.isAssignableFrom(targetClass)
                || Dimension.class.isAssignableFrom(targetClass)) {
            return SourceFactory.createPostGreSqlSource(targetClass.getAnnotation(Table.class).name());
        }
        throw new RuntimeException("target class must implement IJunkDimension, AbstractLookup or Dimension interface");

    }

    @Override
    public ISink getJunkDimensionSink(Class<? extends IJunkDimension> junkDimension) {
        return SinkFactory.createSink(SinkType.POSTGRESQL, junkDimension.getAnnotation(Table.class));
    }

    @Override
    public ISink getJunkDimensionLookupSink(Class<? extends IJunkDimension> junkDimension) {
        return SinkFactory.createSink(SinkType.POSTGRESQL, junkDimension.getAnnotation(Table.class).lookupType().getAnnotation(Table.class));
    }



}
