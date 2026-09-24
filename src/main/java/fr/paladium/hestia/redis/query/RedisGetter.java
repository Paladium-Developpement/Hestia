package fr.paladium.hestia.redis.query;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface RedisGetter<D, R> extends Function<D, R>, Serializable {}