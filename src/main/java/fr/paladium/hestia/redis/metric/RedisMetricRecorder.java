package fr.paladium.hestia.redis.metric;

import lombok.NonNull;

public interface RedisMetricRecorder {

	public void flush();
	public void shutdown();

	public @NonNull RedisMetricGauge gauge(final @NonNull String name);
	public @NonNull RedisMetricTimer timer(final @NonNull String name);
	public @NonNull RedisMetricCounter counter(final @NonNull String name);

}