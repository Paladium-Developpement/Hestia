package fr.paladium.hestia.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import fr.paladium.hestia.cache.transport.CacheMessage;
import fr.paladium.hestia.cache.transport.CacheTransport;
import fr.paladium.hestia.cache.transport.RedisCacheTransport;
import fr.paladium.hestia.store.RedisStore;
import fr.paladium.hestia.store.RedisStoreListener;
import lombok.Getter;
import lombok.NonNull;

public class RedisCache<T> implements RedisStoreListener<T>, AutoCloseable {

	private static final Logger LOGGER = Logger.getLogger(RedisCache.class.getName());

	private final RedisCacheConfig config;
	private final CacheTransport transport;
	@Getter private final RedisStore<T> store;

	private final String origin = UUID.randomUUID().toString();
	private final Map<String, T> values = new ConcurrentHashMap<>();
	private final List<RedisCacheListener<T>> listeners = new CopyOnWriteArrayList<>();
	private final Collection<T> view = Collections.unmodifiableCollection(this.values.values());

	private ScheduledExecutorService scheduler;

	private RedisCache(final @NonNull RedisStore<T> store, final @NonNull RedisCacheConfig config) {
		this.store = store;
		this.config = config;
		this.transport = config.getTransport() != null ? config.getTransport() : RedisCacheTransport.create(store.getClient(), store.getConfig().getName() + ":sync");
	}

	public static @NonNull <T> RedisCache<T> create(final @NonNull RedisStore<T> store, final @NonNull RedisCacheConfig config) {
		if (store.getVersionPath() == null) {
			throw new IllegalArgumentException("A shared cache requires a @RedisJsonVersion field on " + store.getConfig().getType().getName());
		}

		final RedisCache<T> cache = new RedisCache<>(store, config);
		store.listen(cache);
		return cache;
	}

	@Override
	public void close() {
		if (this.scheduler != null) {
			this.scheduler.shutdownNow();
			this.scheduler = null;
		}
		this.transport.close();
	}

	public @NonNull CompletableFuture<Void> start() {
		this.transport.subscribe(this::receive);
		return this.store.fetchAll().thenAccept(objects -> {
			for (final T object : objects) {
				this.update(object);
			}
		}).thenRun(() -> {
			this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
				final Thread thread = new Thread(runnable, "HestiaCacheRefresher-" + this.store.getConfig().getName());
				thread.setDaemon(true);
				return thread;
			});

			final long intervalMs = this.config.getRefreshInterval().toMillis();
			this.scheduler.scheduleAtFixedRate(this::safeRefresh, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
		});
	}

	public @NonNull Collection<T> getAll() {
		return this.view;
	}

	@Override
	public boolean requiresMergedDocument() {
		return true;
	}

	public @NonNull CompletableFuture<Void> refresh() {
		final List<T> snapshot = new ArrayList<>(this.values.values());
		return this.store.fetchVersions().thenCompose(versions -> {
			for (final T before : snapshot) {
				final String id = this.store.getId(before);
				if (!versions.containsKey(id)) {
					this.remove(id, this.store.getVersion(before));
				}
			}

			final List<String> stale = new ArrayList<>();
			for (final Map.Entry<String, Long> entry : versions.entrySet()) {
				final T cached = this.values.get(entry.getKey());
				if (cached == null || this.store.getVersion(cached) < entry.getValue()) {
					stale.add(entry.getKey());
				}
			}
			return this.store.fetchAll(stale);
		}).thenAccept(objects -> {
			for (final T object : objects) {
				this.update(object);
			}
		});
	}

	@Override
	public void onPostDelete(final @NonNull T object) {
		final String id = this.store.getId(object);
		this.remove(id);
		this.publish(new CacheMessage(id, null, 0L, this.origin));
	}

	public @NonNull Optional<T> get(final @NonNull String id) {
		return Optional.ofNullable(this.values.get(id));
	}

	public @NonNull CompletableFuture<Void> invalidate(final @NonNull String id) {
		final T cached = this.values.get(id);
		final CompletableFuture<Boolean> stale = cached == null ? CompletableFuture.completedFuture(true) : this.store.fetchVersion(id).thenApply(version -> version == null || version > this.store.getVersion(cached));
		return stale.thenCompose(refresh -> !refresh ? CompletableFuture.<Void>completedFuture(null) : this.store.fetch(id).thenAccept(object -> {
			if (object == null) {
				this.remove(id);
				return;
			}
			this.update(object);
		}));
	}

	public @NonNull RedisCache<T> listen(final @NonNull RedisCacheListener<T> listener) {
		this.listeners.add(listener);
		return this;
	}

	@Override
	public void onPostSave(final @NonNull T object, final String json, final boolean create) {
		if (json == null) {
			return;
		}

		this.store.parse(json).ifPresent(merged -> {
			this.update(merged);
			this.publish(new CacheMessage(this.store.getId(merged), json, this.store.getVersion(merged), this.origin));
		});
	}

	private void safeRefresh() {
		try {
			this.refresh().whenComplete((result, error) -> {
				if (error != null) {
					RedisCache.LOGGER.log(Level.WARNING, "Refresh of cache '" + this.store.getConfig().getName() + "' failed", error);
				}
			});
		} catch (final Throwable throwable) {
			RedisCache.LOGGER.log(Level.WARNING, "Refresh of cache '" + this.store.getConfig().getName() + "' failed", throwable);
		}
	}

	private void update(final @NonNull T object) {
		final String id = this.store.getId(object);
		final long version = this.store.getVersion(object);
		final AtomicReference<T> previous = new AtomicReference<>();
		final T stored = this.values.compute(id, (key, current) -> {
			if (current != null && this.store.getVersion(current) >= version) {
				return current;
			}

			previous.set(current);
			return object;
		});

		if (stored != object) {
			return;
		}

		for (final RedisCacheListener<T> listener : this.listeners) {
			try {
				listener.onPostUpdate(previous.get(), object);
			} catch (final Throwable throwable) {
				RedisCache.LOGGER.log(Level.WARNING, "Listener of cache '" + this.store.getConfig().getName() + "' failed", throwable);
			}
		}
	}

	private void remove(final @NonNull String id) {
		final T removed = this.values.remove(id);
		if (removed != null) {
			this.fireRemove(removed);
		}
	}

	private void fireRemove(final @NonNull T removed) {
		for (final RedisCacheListener<T> listener : this.listeners) {
			try {
				listener.onPostRemove(removed);
			} catch (final Throwable throwable) {
				RedisCache.LOGGER.log(Level.WARNING, "Listener of cache '" + this.store.getConfig().getName() + "' failed", throwable);
			}
		}
	}

	private void receive(final @NonNull CacheMessage message) {
		if (this.origin.equals(message.getOrigin())) {
			return;
		}

		if (message.getJson() == null) {
			this.remove(message.getId());
			return;
		}

		final T cached = this.values.get(message.getId());
		if (cached != null && this.store.getVersion(cached) >= message.getVersion()) {
			return;
		}

		this.store.parse(message.getJson()).ifPresent(this::update);
	}

	private void publish(final @NonNull CacheMessage message) {
		try {
			this.transport.publish(message);
		} catch (final Throwable throwable) {
			RedisCache.LOGGER.log(Level.WARNING, "Publication of cache '" + this.store.getConfig().getName() + "' failed", throwable);
		}
	}

	private void remove(final @NonNull String id, final long expectedVersion) {
		final AtomicReference<T> removed = new AtomicReference<>();
		this.values.computeIfPresent(id, (key, current) -> {
			if (this.store.getVersion(current) != expectedVersion) {
				return current;
			}

			removed.set(current);
			return null;
		});

		if (removed.get() != null) {
			this.fireRemove(removed.get());
		}
	}

}