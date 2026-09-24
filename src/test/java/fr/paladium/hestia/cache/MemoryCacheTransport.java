package fr.paladium.hestia.cache;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import fr.paladium.hestia.cache.transport.CacheMessage;
import fr.paladium.hestia.cache.transport.CacheTransport;
import lombok.Getter;

public final class MemoryCacheTransport implements CacheTransport {

	private final List<Consumer<CacheMessage>> bus;
	@Getter private final List<CacheMessage> published = new CopyOnWriteArrayList<>();

	private Consumer<CacheMessage> consumer;

	public MemoryCacheTransport(final List<Consumer<CacheMessage>> bus) {
		this.bus = bus;
	}

	@Override
	public void close() {
		this.bus.remove(this.consumer);
	}

	@Override
	public void publish(final CacheMessage message) {
		this.published.add(message);
		for (final Consumer<CacheMessage> subscriber : this.bus) {
			subscriber.accept(message);
		}
	}

	@Override
	public void subscribe(final Consumer<CacheMessage> consumer) {
		this.consumer = consumer;
		this.bus.add(consumer);
	}

}