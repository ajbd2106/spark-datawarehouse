package com.utils;

import org.apache.commons.lang3.Validate;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public class ReflectUtils {

    public static List<Field> getAllFieldsList(Class<?> cls) {
        Validate.isTrue(cls != null, "The class must not be null", new Object[0]);
        List<Field> allFields = new ArrayList();

        for (Class currentClass = cls; currentClass != null; currentClass = currentClass.getSuperclass()) {
            Field[] declaredFields = currentClass.getDeclaredFields();
            Collections.addAll(allFields, declaredFields);
        }

        return allFields;
    }

    public static List<Field> getFieldsListWithAnnotation(Class<?> cls, Class<? extends Annotation> annotationCls) {
        Validate.isTrue(annotationCls != null, "The annotation class must not be null", new Object[0]);
        List<Field> allFields = getAllFieldsList(cls);
        List<Field> annotatedFields = new ArrayList();
        Iterator i$ = allFields.iterator();

        while (i$.hasNext()) {
            Field field = (Field) i$.next();
            if (field.getAnnotation(annotationCls) != null) {
                annotatedFields.add(field);
            }
        }

        return annotatedFields;
    }

    public static List<Field> getSortedFields(List<Class<? extends Annotation>> annotations, Class<?> cls) {
        List<Field> fieldList = getFields(annotations, cls);
        fieldList.sort((Comparator.comparing(Field::getName)));
        return fieldList;
    }

    public static List<Field> getFields(List<Class<? extends Annotation>> annotations, Class<?> cls) {
        return annotations.stream()
                .flatMap(annotation -> getFieldsListWithAnnotation(cls, annotation).stream())
                .collect(Collectors.toList());
    }

    public static Optional<Field> getUniqueField(Class<? extends Annotation> annotation, Class<?> cls) {

        List<Field> fieldList = getFieldsListWithAnnotation(cls, annotation);

        if (fieldList != null && fieldList.size() > 0) {
            return Optional.of(fieldList.get(0));
        }

        return Optional.empty();
    }

    public static void checkExistsAndUnique(Class<? extends Annotation> annotation, Class<?> cls){

        List<Field> fieldList = getFieldsListWithAnnotation(cls, annotation);

        if(fieldList == null || fieldList.size() == 0){
            throw new RuntimeException(String.format("Class %s should have an annotation of type %s", cls.getCanonicalName(), annotation.getSimpleName()));
        }

        if(fieldList.size() > 1){
            throw new RuntimeException(String.format("Cannot have several annotation of type %s on class %s", annotation.getSimpleName(), cls.getCanonicalName()));
        }

    }
}
