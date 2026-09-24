package fr.paladium.hestia.redis.metric;

import java.util.function.Supplier;

import lombok.NonNull;

public interface RedisMetricTimer {

	public void record(final long milliseconds);
	public void record(final @NonNull Runnable operation);
	public <T> T record(final @NonNull Supplier<T> operation);

}