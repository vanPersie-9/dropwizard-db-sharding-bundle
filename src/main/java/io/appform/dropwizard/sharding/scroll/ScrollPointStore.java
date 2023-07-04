package io.appform.dropwizard.sharding.scroll;

import java.util.Optional;

/**
 *
 */
public interface ScrollPointStore {
    Optional<ScrollPointer> save(final String id, int shard, Integer currOffset);
    Optional<ScrollPointer> get(final String id);
}
