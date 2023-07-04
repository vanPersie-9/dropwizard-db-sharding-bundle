package io.appform.dropwizard.sharding.scroll;

import lombok.val;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class InMemoryScrollPointStore implements ScrollPointStore {
    private final Map<String, ScrollPointer> points = new ConcurrentHashMap<>();

    @Override
    public Optional<ScrollPointer> save(String id, int shard, Integer currOffset) {
        return Optional.of(points.compute(id, (key, existing) -> {
            val scrollPoint = null == existing ? new ScrollPointer() : existing;
            scrollPoint.put(shard, currOffset);
            return scrollPoint;
        }));
    }

    @Override
    public Optional<ScrollPointer> get(String id) {
        return Optional.of(points.computeIfAbsent(id, key -> new ScrollPointer()));
    }
}
