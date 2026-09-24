package fr.paladium.hestia.redis.metric;

public interface RedisMetricGauge {

	public void set(final long value);
	public void set(final double value);

}