package io.appform.dropwizard.sharding.listeners;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ListenerContext {
    private Class<?> entityClass;
    private Class<?> daoClass;
    private String shardName;
    private String opType;
    private String lockedContextMode;
}
