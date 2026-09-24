package fr.paladium.hestia.cache.transport;

import java.util.function.Consumer;

import lombok.NonNull;

public interface SharedCacheTransport extends AutoCloseable {

	@Override
	public void close();
	public void publish(final @NonNull SharedCacheMessage message);
	public void subscribe(final @NonNull Consumer<SharedCacheMessage> consumer);

}