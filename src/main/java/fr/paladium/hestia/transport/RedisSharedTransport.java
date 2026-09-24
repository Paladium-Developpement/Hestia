package fr.paladium.hestia.transport;

import java.util.function.Consumer;

import com.google.gson.Gson;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.pubsub.RedisSubscription;
import lombok.NonNull;

public final class RedisSharedTransport implements SharedTransport {

	private static final Gson GSON = new Gson();

	private final String channel;
	private final RedisClient client;

	private RedisSubscription subscription;

	private RedisSharedTransport(final @NonNull RedisClient client, final @NonNull String channel) {
		this.client = client;
		this.channel = channel;
	}

	public static @NonNull RedisSharedTransport create(final @NonNull RedisClient client, final @NonNull String channel) {
		return new RedisSharedTransport(client, channel);
	}

	@Override
	public void close() {
		if (this.subscription != null) {
			this.subscription.close();
			this.subscription = null;
		}
	}

	@Override
	public void publish(final @NonNull SharedMessage message) {
		this.client.execute(RedisCommand.Base.publish(this.channel, RedisSharedTransport.GSON.toJson(message)));
	}

	@Override
	public void subscribe(final @NonNull Consumer<SharedMessage> consumer) {
		this.close();
		this.subscription = this.client.subscribe(this.channel, raw -> consumer.accept(RedisSharedTransport.GSON.fromJson(raw, SharedMessage.class)));
	}

}