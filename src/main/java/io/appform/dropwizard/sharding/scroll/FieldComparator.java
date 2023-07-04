package io.appform.dropwizard.sharding.scroll;

import lombok.SneakyThrows;
import lombok.val;

import java.lang.reflect.Field;
import java.util.Comparator;

/**
 * A {@link Comparator} that is used by scroll api for sorting results from multiple shards
 */
public class FieldComparator<T> implements Comparator<T> {
    private final Field field;

    public FieldComparator(Field field) {
        this.field = field;
    }

    @SneakyThrows
    @Override
    @SuppressWarnings("unchecked")
    public int compare(T lhs, T rhs) {
        val lhsValue = (Comparable<T>)field.get(lhs);
        if(lhsValue == null) {
            return -1;
        }
        if(rhs == null) {
            return 1;
        }

        return lhsValue.compareTo(rhs);
    }
}
