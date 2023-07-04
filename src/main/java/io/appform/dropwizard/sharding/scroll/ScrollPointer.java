package io.appform.dropwizard.sharding.scroll;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Private data structure for scroll point
 */
public class ScrollPointer {
    private final Map<Integer, Integer> pointPerShard = new ConcurrentHashMap<>();

    public ScrollPointer put(int shard, Integer offset) {
        pointPerShard.put(shard, offset);
        return this;
    }

    public int advance(int shard, int advanceBy) {
        return pointPerShard.compute(shard, (key, existing) -> (null == existing ? 0 : existing) + advanceBy);
    }

    public int getCurrOffset(int shard) {
        return pointPerShard.computeIfAbsent(shard, key -> 0);
    }
}
