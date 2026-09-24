package fr.paladium.hestia.redis.metric.impl;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.metric.RedisMetricCounter;
import lombok.NonNull;

public final class RedisMetricAsyncCounter implements RedisMetricCounter {

	private final String key;
	private final long retentionMs;
	private final LongAdder value = new LongAdder();

	public RedisMetricAsyncCounter(final @NonNull String key, final long retentionMs) {
		this.key = key;
		this.retentionMs = retentionMs;
	}

	@Override
	public void increment() {
		this.value.increment();
	}

	@Override
	public void increment(final long value) {
		this.value.add(value);
	}

	public void drain(final @NonNull List<RedisCommand> commands) {
		final long delta = this.value.sumThenReset();
		if (delta == 0L) {
			return;
		}
		commands.add(RedisCommand.TimeSeries.incrBy(this.key, delta, this.retentionMs, "type", "counter"));
	}

}