package fr.paladium.hestia.cache.transport;

import java.util.function.Consumer;

import com.google.gson.Gson;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.pubsub.RedisSubscription;
import lombok.NonNull;

public final class RedisCacheTransport implements CacheTransport {

	private static final Gson GSON = new Gson();

	private final String channel;
	private final RedisClient client;

	private RedisSubscription subscription;

	private RedisCacheTransport(final @NonNull RedisClient client, final @NonNull String channel) {
		this.client = client;
		this.channel = channel;
	}

	public static @NonNull RedisCacheTransport create(final @NonNull RedisClient client, final @NonNull String channel) {
		return new RedisCacheTransport(client, channel);
	}

	@Override
	public void close() {
		if (this.subscription != null) {
			this.subscription.close();
			this.subscription = null;
		}
	}

	@Override
	public void publish(final @NonNull CacheMessage message) {
		this.client.execute(RedisCommand.Base.publish(this.channel, RedisCacheTransport.GSON.toJson(message)));
	}

	@Override
	public void subscribe(final @NonNull Consumer<CacheMessage> consumer) {
		this.close();
		this.subscription = this.client.subscribe(this.channel, raw -> consumer.accept(RedisCacheTransport.GSON.fromJson(raw, CacheMessage.class)));
	}

}