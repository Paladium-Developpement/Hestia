package fr.paladium.hestia.cache.transport;

import java.util.function.Consumer;

import lombok.NonNull;

public interface CacheTransport extends AutoCloseable {

	@Override
	public void close();
	public void publish(final @NonNull CacheMessage message);
	public void subscribe(final @NonNull Consumer<CacheMessage> consumer);

}