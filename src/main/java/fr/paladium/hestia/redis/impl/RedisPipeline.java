package fr.paladium.hestia.redis.impl;

import java.util.List;

import fr.paladium.hestia.redis.metric.RedisMetricRecorder;
import lombok.NonNull;
import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Pipeline;

public final class RedisPipeline {

	private final Pipeline pipeline;
	private final Connection connection;
	private final RedisMetricRecorder metrics;

	private int batchSize;

	private RedisPipeline(final @NonNull ConnectionPool pool, final @NonNull RedisMetricRecorder metrics) {
		this.connection = pool.getResource();
		this.pipeline = new Pipeline(this.connection);
		this.metrics = metrics;
	}

	public static @NonNull RedisPipeline create(final @NonNull ConnectionPool pool, final @NonNull RedisMetricRecorder metrics) {
		return new RedisPipeline(pool, metrics);
	}

	public void sync() {
		final long start = System.nanoTime();
		try {
			this.pipeline.sync();
		} finally {
			this.connection.close();
			this.recordMetrics(start);
		}
	}

	public @NonNull List<Object> execute() {
		final long start = System.nanoTime();
		try {
			return this.pipeline.syncAndReturnAll();
		} finally {
			this.connection.close();
			this.recordMetrics(start);
		}
	}

	public void add(final @NonNull RedisCommand... commands) {
		for (final RedisCommand command : commands) {
			this.pipeline.sendCommand(command.getCommand(), command.getArgs());
		}
		this.batchSize += commands.length;
	}

	private void recordMetrics(final long startNanos) {
		this.metrics.counter("redis.pipeline.execute").increment();
		this.metrics.gauge("redis.pipeline.batch.size").set(this.batchSize);
		this.metrics.timer("redis.pipeline.latency").record((System.nanoTime() - startNanos) / 1_000_000L);
	}

}