package fr.paladium.hestia.transport;

import java.util.function.Consumer;

import lombok.NonNull;

public interface SharedTransport extends AutoCloseable {

	@Override
	public void close();
	public void publish(final @NonNull SharedMessage message);
	public void subscribe(final @NonNull Consumer<SharedMessage> consumer);

}