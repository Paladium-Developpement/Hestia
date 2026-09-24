package fr.paladium.hestia.cache.transport;

import java.util.function.Consumer;

import com.google.gson.Gson;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.pubsub.RedisSubscription;
import lombok.NonNull;

public final class RedisPubSubTransport implements SharedCacheTransport {

	private static final Gson GSON = new Gson();

	private final String channel;
	private final RedisClient client;

	private RedisSubscription subscription;

	private RedisPubSubTransport(final @NonNull RedisClient client, final @NonNull String channel) {
		this.client = client;
		this.channel = channel;
	}

	public static @NonNull RedisPubSubTransport create(final @NonNull RedisClient client, final @NonNull String channel) {
		return new RedisPubSubTransport(client, channel);
	}

	@Override
	public void close() {
		if (this.subscription != null) {
			this.subscription.close();
			this.subscription = null;
		}
	}

	@Override
	public void publish(final @NonNull SharedCacheMessage message) {
		this.client.execute(RedisCommand.Base.publish(this.channel, RedisPubSubTransport.GSON.toJson(message)));
	}

	@Override
	public void subscribe(final @NonNull Consumer<SharedCacheMessage> consumer) {
		this.close();
		this.subscription = this.client.subscribe(this.channel, raw -> consumer.accept(RedisPubSubTransport.GSON.fromJson(raw, SharedCacheMessage.class)));
	}

}