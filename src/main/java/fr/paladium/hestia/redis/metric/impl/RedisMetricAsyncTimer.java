package fr.paladium.hestia.redis.metric.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.metric.RedisMetricTimer;
import lombok.NonNull;

public final class RedisMetricAsyncTimer implements RedisMetricTimer {

	private final String key;
	private final long retentionMs;

	private final AtomicLong sum = new AtomicLong();
	private final AtomicLong count = new AtomicLong();
	private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
	private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

	public RedisMetricAsyncTimer(final @NonNull String key, final long retentionMs) {
		this.key = key;
		this.retentionMs = retentionMs;
	}

	@Override
	public void record(final long milliseconds) {
		this.count.incrementAndGet();
		this.sum.addAndGet(milliseconds);

		long currentMin;
		do {
			currentMin = this.min.get();
			if (milliseconds >= currentMin) {
				break;
			}
		} while (!this.min.compareAndSet(currentMin, milliseconds));

		long currentMax;
		do {
			currentMax = this.max.get();
			if (milliseconds <= currentMax) {
				break;
			}
		} while (!this.max.compareAndSet(currentMax, milliseconds));
	}

	@Override
	public void record(final @NonNull Runnable operation) {
		final long start = System.nanoTime();
		try {
			operation.run();
		} finally {
			this.record((System.nanoTime() - start) / 1_000_000L);
		}
	}

	@Override
	public <T> T record(final @NonNull Supplier<T> operation) {
		final long start = System.nanoTime();
		try {
			return operation.get();
		} finally {
			this.record((System.nanoTime() - start) / 1_000_000L);
		}
	}

	public void drain(final @NonNull List<RedisCommand> commands) {
		final long localCount = this.count.getAndSet(0L);
		if (localCount == 0L) {
			this.min.set(Long.MAX_VALUE);
			this.max.set(Long.MIN_VALUE);
			return;
		}

		final long localSum = this.sum.getAndSet(0L);
		final long localMin = this.min.getAndSet(Long.MAX_VALUE);
		final long localMax = this.max.getAndSet(Long.MIN_VALUE);

		commands.add(RedisCommand.TimeSeries.add(this.key + ".count", localCount, this.retentionMs, "type", "timer_count"));
		commands.add(RedisCommand.TimeSeries.add(this.key + ".sum", localSum, this.retentionMs, "type", "timer_sum"));
		commands.add(RedisCommand.TimeSeries.add(this.key + ".min", localMin, this.retentionMs, "type", "timer_min"));
		commands.add(RedisCommand.TimeSeries.add(this.key + ".max", localMax, this.retentionMs, "type", "timer_max"));
	}

}