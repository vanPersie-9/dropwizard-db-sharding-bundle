package io.appform.dropwizard.sharding.scroll;

import lombok.Value;

/**
 *
 */
@Value
public class ScrollSort {
    public enum Order {
        ASC,
        DESC
    }
    String fieldName;
    Order order;
}
