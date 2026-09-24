package fr.paladium.hestia.redis.metric;

public interface RedisMetricCounter {

	public void increment();
	public void increment(final long value);

}