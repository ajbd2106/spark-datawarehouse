package com.hadoopunit.test.comparator;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.util.*;

public class ReflectComparator<T> implements Comparator<T> {

    private final Collection<String> fieldNames;
    private final Class<T> currentClass;

    public ReflectComparator(Collection<String> fieldNames, Class<T> currentClass) {
        this.fieldNames = fieldNames;
        this.currentClass = currentClass;
    }

    @Override
    public int compare(T t, T t1) {

        for (String fieldName : fieldNames) {
            try {
                Field field = FieldUtils.getField(currentClass,fieldName, true );
                Object object1 = FieldUtils.readField(field, t, true);
                Object object2 = FieldUtils.readField(field, t1, true);

                if(!object1.equals(object2)){
                    return Integer.compare(t.hashCode(), t1.hashCode());
                }
            } catch (IllegalAccessException e) {
                Assert.assertTrue(String.format("cannot access field %s for class %s", fieldName, currentClass.getCanonicalName()), false);
            }

        }

        return 0;
    }

    public List<T> retainAll(List<T> collection, List<T> retain){
        List list = new ArrayList(Math.min(collection.size(), retain.size()));

        Iterator<T> retainIterator = retain.iterator();
        while(retainIterator.hasNext()) {
            T retainObject = retainIterator.next();

            Iterator<T> iter = collection.iterator();
            while(iter.hasNext()){
                T currentObject = iter.next();
                if (compare(currentObject, retainObject ) == 0) {
                    list.add(currentObject);
                }
            }
        }

        return list;
    }
}
