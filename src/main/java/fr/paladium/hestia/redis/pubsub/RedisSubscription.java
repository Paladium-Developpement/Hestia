package fr.paladium.hestia.redis.pubsub;

import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.RedisConfig;
import lombok.Getter;
import lombok.NonNull;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public final class RedisSubscription implements AutoCloseable {

	private static final long RECONNECT_MS = 1000L;
	private static final Logger LOGGER = Logger.getLogger(RedisSubscription.class.getName());

	private final Thread thread;
	private final RedisConfig config;
	@Getter private final String channel;
	private final Consumer<String> listener;

	private volatile Jedis jedis;
	private volatile boolean closed;

	private RedisSubscription(final @NonNull RedisConfig config, final @NonNull String channel, final @NonNull Consumer<String> listener) {
		this.config = config;
		this.channel = channel;
		this.listener = listener;
		this.thread = new Thread(this::run, "HestiaSubscription-" + channel);
		this.thread.setDaemon(true);
	}

	public static @NonNull RedisSubscription create(final @NonNull RedisConfig config, final @NonNull String channel, final @NonNull Consumer<String> listener) {
		final RedisSubscription subscription = new RedisSubscription(config, channel, listener);
		subscription.thread.start();
		return subscription;
	}

	@Override
	public void close() {
		this.closed = true;
		final Jedis current = this.jedis;
		if (current != null) {
			current.disconnect();
		}
		this.thread.interrupt();
	}

	private void run() {
		while (!this.closed) {
			try (Jedis connection = new Jedis(new HostAndPort(this.config.getHost(), this.config.getPort()), RedisClient.clientConfig(this.config))) {
				this.jedis = connection;
				if (this.closed) {
					return;
				}

				connection.subscribe(new JedisPubSub() {

					@Override
					public void onMessage(final String name, final String message) {
						RedisSubscription.this.dispatch(message);
					}

				}, this.channel);
			} catch (final Exception exception) {
				if (this.closed) {
					return;
				}

				RedisSubscription.LOGGER.log(Level.WARNING, "Subscription to '" + this.channel + "' lost, reconnecting", exception);
				try {
					Thread.sleep(RedisSubscription.RECONNECT_MS);
				} catch (final InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		}
	}

	private void dispatch(final String message) {
		try {
			this.listener.accept(message);
		} catch (final Throwable throwable) {
			RedisSubscription.LOGGER.log(Level.WARNING, "Subscription listener on '" + this.channel + "' failed", throwable);
		}
	}

}