package com.source;

import com.sink.ISink;
import com.starschema.dimension.junk.IJunkDimension;
import com.starschema.dimension.junk.JunkDimension;

import java.io.Serializable;

public interface IFactSourceFactory extends Serializable {
    ISource getJunkDimensionSource(Class<? extends IJunkDimension> junkDimension);
    ISource getLookupSource(Class<?> targetClass);

    ISink getJunkDimensionSink(Class<? extends IJunkDimension> junkDimension);
    ISink getJunkDimensionLookupSink(Class<? extends IJunkDimension> junkDimension);
}
