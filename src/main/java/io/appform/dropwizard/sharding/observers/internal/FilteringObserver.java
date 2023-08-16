package io.appform.dropwizard.sharding.observers.internal;

import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.filters.FilterOutput;
import io.appform.dropwizard.sharding.filters.TransactionFilter;
import io.appform.dropwizard.sharding.filters.TransactionFilterResult;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import lombok.Getter;
import lombok.val;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 *
 */
@Getter
public class FilteringObserver extends TransactionObserver {

    private final List<TransactionFilter> filters = new ArrayList<>();

    public FilteringObserver(TransactionObserver next) {
        super(next);
    }

    public FilteringObserver addFilter(final TransactionFilter filter) {
        filters.add(filter);
        return this;
    }

    public FilteringObserver addFilters(final TransactionFilter ...filters) {
        return addFilters(Arrays.asList(filters));
    }

    public FilteringObserver addFilters(final Collection<TransactionFilter> filters) {
        this.filters.addAll(filters);
        return this;
    }

    @Override
    public final <T> T execute(TransactionExecutionContext context, Supplier<T> supplier) {
        val blocks = filters.stream()
                .map(filter -> filter.evaluate(context))
                .filter(result -> FilterOutput.BLOCK.equals(result.getOutput()))
                .map(TransactionFilterResult::getReason)
                .collect(Collectors.toList());
        if(blocks.isEmpty()) {
            return proceed(context, supplier);
        }
        throw new TransactionFilteredException(blocks);
    }
}
