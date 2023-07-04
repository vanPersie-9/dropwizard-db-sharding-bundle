package io.appform.dropwizard.sharding.scroll;

import lombok.SneakyThrows;
import lombok.val;

import java.lang.reflect.Field;
import java.util.Comparator;

/**
 *
 */
public class FieldComparator<T> implements Comparator<T> {
    private final Field field;

    public FieldComparator(Field field) {
        this.field = field;
    }

    @SneakyThrows
    @Override
    public int compare(T lhs, T rhs) {
        val lhsValue = (Comparable)field.get(lhs);
        val rhsValue = (Comparable) field.get(rhs);
        if(lhsValue == null) {
            return -1;
        }
        if(rhsValue == null) {
            return 1;
        }

        return lhsValue.compareTo(rhsValue);
    }
}
