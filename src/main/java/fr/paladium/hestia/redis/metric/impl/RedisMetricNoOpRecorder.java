package fr.paladium.hestia.redis.metric.impl;

import java.util.function.Supplier;

import fr.paladium.hestia.redis.metric.RedisMetricCounter;
import fr.paladium.hestia.redis.metric.RedisMetricGauge;
import fr.paladium.hestia.redis.metric.RedisMetricRecorder;
import fr.paladium.hestia.redis.metric.RedisMetricTimer;
import lombok.NonNull;

public final class RedisMetricNoOpRecorder implements RedisMetricRecorder {

	public static final RedisMetricNoOpRecorder INSTANCE = new RedisMetricNoOpRecorder();

	private static final RedisMetricGauge GAUGE = new RedisMetricGauge() {

		@Override
		public void set(final long value) {}

		@Override
		public void set(final double value) {}

	};

	private static final RedisMetricTimer TIMER = new RedisMetricTimer() {

		@Override
		public void record(final long milliseconds) {}

		@Override
		public void record(final @NonNull Runnable operation) {
			operation.run();
		}

		@Override
		public <T> T record(final @NonNull Supplier<T> operation) {
			return operation.get();
		}

	};

	private static final RedisMetricCounter COUNTER = new RedisMetricCounter() {

		@Override
		public void increment() {}

		@Override
		public void increment(final long value) {}

	};

	private RedisMetricNoOpRecorder() {}

	@Override
	public void shutdown() {}

	@Override
	public void flush() {}

	@Override
	public @NonNull RedisMetricGauge gauge(final @NonNull String name) {
		return RedisMetricNoOpRecorder.GAUGE;
	}

	@Override
	public @NonNull RedisMetricTimer timer(final @NonNull String name) {
		return RedisMetricNoOpRecorder.TIMER;
	}

	@Override
	public @NonNull RedisMetricCounter counter(final @NonNull String name) {
		return RedisMetricNoOpRecorder.COUNTER;
	}

}