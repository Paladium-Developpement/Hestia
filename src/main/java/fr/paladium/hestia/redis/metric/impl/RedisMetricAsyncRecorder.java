package fr.paladium.hestia.redis.metric.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.metric.RedisMetricConfig;
import fr.paladium.hestia.redis.metric.RedisMetricCounter;
import fr.paladium.hestia.redis.metric.RedisMetricGauge;
import fr.paladium.hestia.redis.metric.RedisMetricRecorder;
import fr.paladium.hestia.redis.metric.RedisMetricTimer;
import lombok.NonNull;
import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Pipeline;

public final class RedisMetricAsyncRecorder implements RedisMetricRecorder {

	private static final Logger LOGGER = Logger.getLogger(RedisMetricAsyncRecorder.class.getName());

	private final long retentionMs;
	private final ConnectionPool pool;
	private final RedisMetricConfig config;

	private final Map<String, RedisMetricAsyncTimer> timers = new ConcurrentHashMap<>();
	private final Map<String, RedisMetricAsyncGauge> gauges = new ConcurrentHashMap<>();
	private final Map<String, RedisMetricAsyncCounter> counters = new ConcurrentHashMap<>();

	private ScheduledExecutorService scheduler;

	private RedisMetricAsyncRecorder(final @NonNull ConnectionPool pool, final @NonNull RedisMetricConfig config) {
		this.pool = pool;
		this.config = config;
		this.retentionMs = config.getRetention().toMillis();
	}

	public static @NonNull RedisMetricAsyncRecorder create(final @NonNull ConnectionPool pool, final @NonNull RedisMetricConfig config) {
		return new RedisMetricAsyncRecorder(pool, config);
	}

	public void start() {
		if (this.scheduler != null) {
			return;
		}

		this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
			final Thread thread = new Thread(runnable, "HestiaMetricFlusher");
			thread.setDaemon(true);
			return thread;
		});

		final long intervalMs = this.config.getFlushInterval().toMillis();
		this.scheduler.scheduleAtFixedRate(this::safeFlush, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
	}

	@Override
	public void shutdown() {
		if (this.scheduler != null) {
			this.scheduler.shutdownNow();
			this.scheduler = null;
		}
		this.flush();
	}

	@Override
	public void flush() {
		final List<RedisCommand> commands = new ArrayList<>();
		for (final RedisMetricAsyncCounter counter : this.counters.values()) {
			counter.drain(commands);
		}

		for (final RedisMetricAsyncGauge gauge : this.gauges.values()) {
			gauge.drain(commands);
		}

		for (final RedisMetricAsyncTimer timer : this.timers.values()) {
			timer.drain(commands);
		}

		if (commands.isEmpty()) {
			return;
		}

		try (Connection connection = this.pool.getResource()) {
			final Pipeline pipeline = new Pipeline(connection);
			for (final RedisCommand command : commands) {
				pipeline.sendCommand(command.getCommand(), command.getArgs());
			}
			pipeline.sync();
		} catch (final Exception exception) {
			RedisMetricAsyncRecorder.LOGGER.log(Level.WARNING, "Metric flush failed", exception);
		}
	}

	@Override
	public @NonNull RedisMetricGauge gauge(final @NonNull String name) {
		return this.gauges.computeIfAbsent(name, key -> new RedisMetricAsyncGauge(this.buildKey(key), this.retentionMs));
	}

	@Override
	public @NonNull RedisMetricTimer timer(final @NonNull String name) {
		return this.timers.computeIfAbsent(name, key -> new RedisMetricAsyncTimer(this.buildKey(key), this.retentionMs));
	}

	@Override
	public @NonNull RedisMetricCounter counter(final @NonNull String name) {
		return this.counters.computeIfAbsent(name, key -> new RedisMetricAsyncCounter(this.buildKey(key), this.retentionMs));
	}

	private void safeFlush() {
		try {
			this.flush();
		} catch (final Throwable throwable) {
			RedisMetricAsyncRecorder.LOGGER.log(Level.WARNING, "Metric flush failed", throwable);
		}
	}

	private @NonNull String buildKey(final @NonNull String name) {
		return this.config.getPrefix() + ":" + name;
	}

}