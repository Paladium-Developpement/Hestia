package fr.paladium.hestia.store;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;

import lombok.Getter;
import lombok.NonNull;

@Getter
@SuppressWarnings("unchecked")
public class RedisStoreConfig<T> {

	private final String name;
	private final Class<T> type;
	private final Function<T, String> identifier;

	private int threads = 128;
	private int batchSize = 100;
	private int patchAttempts = 5;
	private Duration readFlushInterval = Duration.ZERO;
	private Duration writeFlushInterval = Duration.ZERO;
	private Duration patchBackoff = Duration.ofMillis(250L);
	private Predicate<String> keyFilter = id -> id.indexOf(':') < 0;

	protected RedisStoreConfig(final @NonNull String name, final @NonNull Class<T> type, final @NonNull Function<T, String> identifier) {
		this.name = name;
		this.type = type;
		this.identifier = identifier;
	}

	public static @NonNull <T> RedisStoreConfig<T> create(final @NonNull Class<T> type, final @NonNull Function<T, String> identifier) {
		return new RedisStoreConfig<>(type.getSimpleName().toLowerCase(), type, identifier);
	}

	public static @NonNull <T> RedisStoreConfig<T> create(final @NonNull String name, final @NonNull Class<T> type, final @NonNull Function<T, String> identifier) {
		return new RedisStoreConfig<>(name, type, identifier);
	}

	public final @NonNull <C extends RedisStoreConfig<T>> C threads(final int threads) {
		this.threads = threads;
		return (C) this;
	}

	public final @NonNull <C extends RedisStoreConfig<T>> C batchSize(final int batchSize) {
		this.batchSize = batchSize;
		return (C) this;
	}

	public final @NonNull <C extends RedisStoreConfig<T>> C patchAttempts(final int patchAttempts) {
		this.patchAttempts = patchAttempts;
		return (C) this;
	}

	public final @NonNull <C extends RedisStoreConfig<T>> C patchBackoff(final @NonNull Duration patchBackoff) {
		this.patchBackoff = patchBackoff;
		return (C) this;
	}

	public final @NonNull <C extends RedisStoreConfig<T>> C keyFilter(final @NonNull Predicate<String> keyFilter) {
		this.keyFilter = keyFilter;
		return (C) this;
	}

	public final @NonNull <C extends RedisStoreConfig<T>> C readFlushInterval(final @NonNull Duration readFlushInterval) {
		this.readFlushInterval = readFlushInterval;
		return (C) this;
	}

	public final @NonNull <C extends RedisStoreConfig<T>> C writeFlushInterval(final @NonNull Duration writeFlushInterval) {
		this.writeFlushInterval = writeFlushInterval;
		return (C) this;
	}

}