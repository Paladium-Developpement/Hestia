package fr.paladium.hestia.redis.metric.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.metric.RedisMetricGauge;
import lombok.NonNull;

public final class RedisMetricAsyncGauge implements RedisMetricGauge {

	private final String key;
	private final long retentionMs;

	private final AtomicLong value = new AtomicLong();
	private final AtomicBoolean dirty = new AtomicBoolean();

	public RedisMetricAsyncGauge(final @NonNull String key, final long retentionMs) {
		this.key = key;
		this.retentionMs = retentionMs;
	}

	@Override
	public void set(final long value) {
		this.value.set(Double.doubleToRawLongBits(value));
		this.dirty.set(true);
	}

	@Override
	public void set(final double value) {
		this.value.set(Double.doubleToRawLongBits(value));
		this.dirty.set(true);
	}

	public void drain(final @NonNull List<RedisCommand> commands) {
		if (!this.dirty.compareAndSet(true, false)) {
			return;
		}

		final double current = Double.longBitsToDouble(this.value.get());
		commands.add(RedisCommand.TimeSeries.add(this.key, current, this.retentionMs, "type", "gauge"));
	}

}