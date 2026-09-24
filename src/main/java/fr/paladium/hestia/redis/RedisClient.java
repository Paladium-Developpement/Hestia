package fr.paladium.hestia.redis;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import fr.paladium.hestia.redis.exception.RedisConnectionException;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.impl.RedisPipeline;
import fr.paladium.hestia.redis.impl.RedisProtocol;
import fr.paladium.hestia.redis.index.RedisIndexResolver;
import fr.paladium.hestia.redis.json.RedisJsonSerializer;
import fr.paladium.hestia.redis.json.RedisJsonTransientExclusionStrategy;
import fr.paladium.hestia.redis.lock.RedisLock;
import fr.paladium.hestia.redis.metric.RedisMetricRecorder;
import fr.paladium.hestia.redis.metric.impl.RedisMetricAsyncRecorder;
import fr.paladium.hestia.redis.metric.impl.RedisMetricNoOpRecorder;
import fr.paladium.hestia.redis.pubsub.RedisSubscription;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

@Getter
public class RedisClient implements AutoCloseable {

	private final RedisLock lock;
	private final RedisConfig config;
	private final RedisMetricRecorder metrics;
	private final RedisJsonSerializer jsonSerializer;
	@Getter(AccessLevel.NONE) private final ConnectionPool pool;

	protected RedisClient(final @NonNull RedisConfig config) throws RedisConnectionException {
		final GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setJmxEnabled(false);
		poolConfig.setMinIdle(2);
		poolConfig.setMaxIdle(config.getPoolSize());
		poolConfig.setMaxTotal(config.getPoolSize());
		poolConfig.setTestOnBorrow(false);
		poolConfig.setTestWhileIdle(true);
		poolConfig.setMinEvictableIdleTime(Duration.ofSeconds(60L));
		poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30L));

		this.config = config;
		this.pool = new ConnectionPool(new HostAndPort(config.getHost(), config.getPort()), RedisClient.clientConfig(config), poolConfig);
		try (Connection connection = this.pool.getResource()) {
			connection.sendCommand(RedisProtocol.PING);
			connection.getOne();
		} catch (final Exception exception) {
			this.pool.close();
			throw new RedisConnectionException("Failed to connect to Redis at '" + config.getHost() + ":" + config.getPort() + "'", exception);
		}

		this.lock = RedisLock.create(this);
		this.jsonSerializer = RedisJsonSerializer.create(config.getGson().newBuilder().addSerializationExclusionStrategy(RedisJsonTransientExclusionStrategy.INSTANCE).create(), config.getTypeResolver());
		if (config.getMetrics() == null) {
			this.metrics = RedisMetricNoOpRecorder.INSTANCE;
		} else {
			final RedisMetricAsyncRecorder recorder = RedisMetricAsyncRecorder.create(this.pool, config.getMetrics());
			recorder.start();
			this.metrics = recorder;
		}
	}

	public static @NonNull RedisClient create(final @NonNull RedisConfig config) throws RedisConnectionException {
		return new RedisClient(config);
	}

	@Override
	public void close() {
		this.lock.close();
		this.metrics.shutdown();
		this.pool.close();
	}

	public @NonNull RedisPipeline pipeline() {
		return RedisPipeline.create(this.pool, this.metrics);
	}

	public boolean index(final @NonNull Class<?> clazz) {
		final RedisCommand command = RedisIndexResolver.resolve(clazz);
		if (command == null) {
			return false;
		}

		try {
			this.execute(command);
			return true;
		} catch (final JedisDataException exception) {
			if (exception.getMessage() != null && exception.getMessage().contains("already exists")) {
				return false;
			}
			throw exception;
		}
	}

	public <T> T execute(final @NonNull RedisCommand command) {
		final long start = System.nanoTime();
		final String name = command.getCommand().name().toLowerCase();
		try (Connection connection = this.pool.getResource()) {
			connection.sendCommand(command.getCommand(), command.getArgs());
			final T result = command.parseResponse(connection.getOne());
			this.metrics.counter("redis.command." + name).increment();
			this.metrics.timer("redis.command." + name + ".latency").record((System.nanoTime() - start) / 1_000_000L);
			return result;
		} catch (final Throwable throwable) {
			this.metrics.counter("redis.command." + name + ".failed").increment();
			throw throwable;
		}
	}

	@SuppressWarnings("unchecked")
	public @NonNull List<String> scan(final @NonNull String pattern) {
		final long start = System.nanoTime();
		final List<String> keys = new ArrayList<>();
		try (Connection connection = this.pool.getResource()) {
			String cursor = "0";
			do {
				connection.sendCommand(RedisProtocol.SCAN, cursor, "MATCH", pattern, "COUNT", "1000");
				final List<Object> result = (List<Object>) connection.getOne();
				cursor = new String((byte[]) result.get(0), StandardCharsets.UTF_8);
				for (final Object key : (List<Object>) result.get(1)) {
					keys.add(new String((byte[]) key, StandardCharsets.UTF_8));
				}
			} while (!"0".equals(cursor));
		} catch (final Throwable throwable) {
			this.metrics.counter("redis.scan.failed").increment();
			throw throwable;
		}

		this.metrics.counter("redis.scan").increment();
		this.metrics.timer("redis.scan.latency").record((System.nanoTime() - start) / 1_000_000L);
		return keys;
	}

	public @NonNull List<Object> execute(final @NonNull RedisCommand... commands) {
		if (commands.length == 0) {
			return new ArrayList<>();
		}

		if (commands.length == 1) {
			final List<Object> result = new ArrayList<>(1);
			result.add(this.execute(commands[0]));
			return result;
		}

		final long start = System.nanoTime();
		final List<Object> results;
		try (Connection connection = this.pool.getResource()) {
			final Pipeline pipeline = new Pipeline(connection);
			for (final RedisCommand command : commands) {
				pipeline.sendCommand(command.getCommand(), command.getArgs());
			}

			final List<Object> raw = pipeline.syncAndReturnAll();
			results = new ArrayList<>(commands.length);
			for (int i = 0; i < commands.length; i++) {
				final Object value = raw.get(i);
				if (value instanceof RuntimeException) {
					throw (RuntimeException) value;
				}
				results.add(commands[i].parseResponse(value));
			}
		} catch (final Throwable throwable) {
			this.metrics.counter("redis.execute.batch.failed").increment();
			throw throwable;
		}

		this.metrics.counter("redis.execute.batch").increment();
		this.metrics.gauge("redis.execute.batch.size").set(commands.length);
		this.metrics.timer("redis.execute.batch.latency").record((System.nanoTime() - start) / 1_000_000L);
		return results;
	}

	public @NonNull RedisSubscription subscribe(final @NonNull String channel, final @NonNull Consumer<String> listener) {
		return RedisSubscription.create(this.config, channel, listener);
	}

	public static boolean isTransient(final @NonNull Throwable throwable) {
		if (throwable instanceof JedisConnectionException) {
			return true;
		}

		if (!(throwable instanceof JedisDataException) || throwable.getMessage() == null) {
			return false;
		}

		final String message = throwable.getMessage();
		return message.startsWith("OOM") || message.startsWith("BUSY") || message.startsWith("LOADING") || message.startsWith("TRYAGAIN") || message.startsWith("READONLY") || message.startsWith("MASTERDOWN");
	}

	public static @NonNull JedisClientConfig clientConfig(final @NonNull RedisConfig config) {
		return DefaultJedisClientConfig.builder().password(config.getPassword()).database(config.getDatabase()).build();
	}

}