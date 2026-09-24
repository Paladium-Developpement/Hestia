package fr.paladium.hestia.store;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.impl.RedisPipeline;
import fr.paladium.hestia.redis.impl.RedisResponse;
import fr.paladium.hestia.redis.index.RedisIndexResolver;
import fr.paladium.hestia.redis.json.RedisJsonPatch;
import fr.paladium.hestia.redis.json.RedisJsonSerializer;
import fr.paladium.hestia.redis.json.utils.RedisJsonSerializeResult;
import fr.paladium.hestia.redis.lock.RedisLockLostException;
import fr.paladium.hestia.redis.lock.RedisLockToken;
import fr.paladium.hestia.redis.query.RedisQuery;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public class SharedStore<T> implements AutoCloseable {

	private static final Logger LOGGER = Logger.getLogger(SharedStore.class.getName());
	private static final String DELETE_SCRIPT = "if redis.call('GET', KEYS[2]) ~= ARGV[1] then return -1 end return redis.call('DEL', KEYS[1])";

	private final String prefix;
	private final ExecutorService executor;
	@Getter private final String versionPath;
	@Getter private final RedisClient client;
	private final ScheduledExecutorService scheduler;
	@Getter private final SharedStoreConfig<T> config;

	private final Queue<Request> queue = new ConcurrentLinkedQueue<>();
	private final Map<String, Request> requests = new ConcurrentHashMap<>();
	private final List<SharedStoreListener<T>> listeners = new CopyOnWriteArrayList<>();
	private final Map<String, CompletableFuture<Void>> operations = new ConcurrentHashMap<>();

	private SharedStore(final @NonNull RedisClient client, final @NonNull SharedStoreConfig<T> config) {
		final Field versionField = RedisJsonSerializer.resolveVersionField(config.getType());
		this.client = client;
		this.config = config;
		this.prefix = config.getName() + ":";
		this.versionPath = versionField == null ? null : RedisCommand.ROOT_PATH + "." + versionField.getName();
		this.executor = SharedStore.createExecutor(config);
		this.scheduler = SharedStore.createScheduler(config);
		if (!config.getFlushInterval().isZero() && !config.getFlushInterval().isNegative()) {
			final long intervalMs = config.getFlushInterval().toMillis();
			this.scheduler.scheduleAtFixedRate(this::safeFlush, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
		}
	}

	public static @NonNull <T> SharedStore<T> create(final @NonNull RedisClient client, final @NonNull SharedStoreConfig<T> config) {
		return new SharedStore<>(client, config);
	}

	@Override
	public void close() {
		this.scheduler.shutdownNow();
		this.flush();
		this.executor.shutdown();
		try {
			this.executor.awaitTermination(30L, TimeUnit.SECONDS);
		} catch (final InterruptedException exception) {
			Thread.currentThread().interrupt();
		}
	}

	public void flush() {
		final List<Request> batch = new ArrayList<>();
		Request request;
		while ((request = this.queue.poll()) != null) {
			batch.add(request);
			if (request.getKey() != null) {
				this.requests.remove(request.getKey(), request);
			}
		}

		if (batch.isEmpty()) {
			return;
		}

		this.client.getMetrics().gauge(this.metric("tick.batch.size")).set(batch.size());
		this.client.getMetrics().counter(this.metric("tick.total")).increment();
		CompletableFuture.runAsync(() -> this.client.getMetrics().timer(this.metric("tick.latency")).record(() -> this.execute(batch)), this.executor).whenComplete((result, error) -> {
			if (error == null) {
				return;
			}

			this.client.getMetrics().counter(this.metric("tick.failed")).increment();
			for (final Request failed : batch) {
				failed.getFuture().completeExceptionally(error);
			}
		});
	}

	public boolean index() {
		return this.client.index(this.config.getType());
	}

	public long versionOf(final @NonNull T object) {
		return this.client.getJsonSerializer().getVersion(object);
	}

	public @NonNull CompletableFuture<List<T>> getAll() {
		this.client.getMetrics().counter(this.metric("getAll.total")).increment();
		return CompletableFuture.supplyAsync(() -> this.client.getMetrics().timer(this.metric("getAll.latency")).record(() -> this.fetch(this.scanKeys())), this.executor);
	}

	public @NonNull String idOf(final @NonNull T object) {
		return this.config.getIdentifier().apply(object);
	}

	public @NonNull Optional<T> parse(final String json) {
		if (json == null) {
			return Optional.empty();
		}

		final JsonElement tree = JsonParser.parseString(json);
		final T object = this.client.getJsonSerializer().getGson().fromJson(tree, this.config.getType());
		if (object == null) {
			return Optional.empty();
		}

		for (final SharedStoreListener<T> listener : this.listeners) {
			listener.onLoad(object);
		}

		this.client.getJsonSerializer().bind(object, tree);
		this.client.getJsonSerializer().snapshot(this.keyOf(this.idOf(object)), tree, this.config.getType());
		return Optional.of(object);
	}

	public @NonNull String keyOf(final @NonNull String id) {
		return this.prefix + id;
	}

	public @NonNull CompletableFuture<T> get(final @NonNull String id) {
		this.client.getMetrics().counter(this.metric("get.total")).increment();
		return this.<String>queue(this.prefix + "get:" + id, RedisCommand.Json.get(this.keyOf(id))).thenApply(json -> this.parse(json).orElse(null));
	}

	public @NonNull CompletableFuture<Map<String, Long>> getVersions() {
		final String path = this.requireVersionPath();
		this.client.getMetrics().counter(this.metric("getVersions.total")).increment();
		return CompletableFuture.supplyAsync(() -> this.client.getMetrics().timer(this.metric("getVersions.latency")).record(() -> {
			final List<String> keys = this.scanKeys();
			final Map<String, Long> versions = new HashMap<>(keys.size());
			if (keys.isEmpty()) {
				return versions;
			}

			final List<String> values = this.client.execute(RedisCommand.Json.mget(path, keys.toArray(new String[0])).withResponse(RedisResponse.stringList()));
			for (int i = 0; i < keys.size(); i++) {
				final Long version = SharedStore.parseVersion(values.get(i));
				if (version != null) {
					versions.put(keys.get(i).substring(this.prefix.length()), version);
				}
			}
			return versions;
		}), this.executor);
	}

	public @NonNull CompletableFuture<Void> save(final @NonNull T object) {
		return this.save(object, null);
	}

	public @NonNull CompletableFuture<Void> delete(final @NonNull T object) {
		return this.delete(object, null);
	}

	public @NonNull CompletableFuture<T> findOne(final @NonNull String query) {
		this.client.getMetrics().counter(this.metric("find.total")).increment();
		final RedisCommand command = RedisCommand.Search.search(RedisIndexResolver.indexName(this.config.getType()), query);
		return this.<String>queue(this.prefix + "find:" + query, command).thenApply(json -> this.parse(json).orElse(null));
	}

	public @NonNull CompletableFuture<Long> getVersion(final @NonNull String id) {
		final RedisCommand command = RedisCommand.Json.get(this.keyOf(id), this.requireVersionPath());
		return this.<String>queue(this.prefix + "version:" + id, command).thenApply(SharedStore::parseVersion);
	}

	public @NonNull <R> CompletableFuture<R> queue(final @NonNull RedisQuery<R> query) {
		return this.queue(query.cacheKey(), query.toCommand(this.client.getJsonSerializer().getGson()));
	}

	public @NonNull <R> CompletableFuture<R> queue(final @NonNull RedisCommand command) {
		return this.submit(null, command).thenApply(command::parseResponse);
	}

	public @NonNull SharedStore<T> listen(final @NonNull SharedStoreListener<T> listener) {
		this.listeners.add(listener);
		return this;
	}

	public @NonNull CompletableFuture<List<T>> getAll(final @NonNull Collection<String> ids) {
		final List<String> keys = new ArrayList<>(ids.size());
		for (final String id : ids) {
			keys.add(this.keyOf(id));
		}
		return CompletableFuture.supplyAsync(() -> this.fetch(keys), this.executor);
	}

	public @NonNull CompletableFuture<Void> save(final @NonNull T object, final RedisLockToken token) {
		final String key = this.keyOf(this.idOf(object));
		final boolean create = !this.client.getJsonSerializer().hasSnapshot(key);
		final JsonElement captured;
		try {
			if (this.isCancelled(listener -> listener.onPreSave(object, create))) {
				return CompletableFuture.completedFuture(null);
			}
			captured = this.client.getJsonSerializer().capture(object);
		} catch (final Throwable throwable) {
			return SharedStore.failed(throwable);
		}

		this.client.getMetrics().counter(this.metric("save.total")).increment();
		return this.chain(key, () -> this.doSave(object, key, captured, token));
	}

	public @NonNull CompletableFuture<Void> delete(final @NonNull T object, final RedisLockToken token) {
		try {
			if (this.isCancelled(listener -> listener.onPreDelete(object))) {
				return CompletableFuture.completedFuture(null);
			}
		} catch (final Throwable throwable) {
			return SharedStore.failed(throwable);
		}

		this.client.getMetrics().counter(this.metric("delete.total")).increment();
		final String key = this.keyOf(this.idOf(object));
		return this.chain(key, () -> this.doDelete(object, key, token));
	}

	public @NonNull <R> CompletableFuture<R> queue(final String key, final @NonNull RedisCommand command) {
		return this.submit(key, command).thenApply(command::parseResponse);
	}

	private void safeFlush() {
		try {
			this.flush();
		} catch (final Throwable throwable) {
			SharedStore.LOGGER.log(Level.WARNING, "Flush of store '" + this.config.getName() + "' failed", throwable);
		}
	}

	private @NonNull List<String> scanKeys() {
		final List<String> keys = new ArrayList<>();
		for (final String key : this.client.scan(this.prefix + "*")) {
			if (this.config.getKeyFilter().test(key.substring(this.prefix.length()))) {
				keys.add(key);
			}
		}
		return keys;
	}

	private @NonNull String requireVersionPath() {
		if (this.versionPath == null) {
			throw new IllegalStateException("Store '" + this.config.getName() + "' requires a @RedisJsonVersion field on " + this.config.getType().getName());
		}
		return this.versionPath;
	}

	private void dispatch(final @NonNull Runnable task) {
		if (this.executor.isShutdown()) {
			task.run();
			return;
		}
		this.executor.execute(task);
	}

	private String fetchJson(final @NonNull String key) {
		try {
			return this.client.execute(RedisCommand.Json.get(key));
		} catch (final RuntimeException exception) {
			return null;
		}
	}

	private void execute(final @NonNull List<Request> batch) {
		final RedisPipeline pipeline = this.client.pipeline();
		for (final Request request : batch) {
			pipeline.add(request.getCommand());
		}

		final List<Object> results = pipeline.execute();
		for (int i = 0; i < batch.size(); i++) {
			final Request request = batch.get(i);
			final Object result = results.get(i);
			this.dispatch(() -> request.complete(result));
		}
	}

	private String apply(final @NonNull RedisJsonPatch patch) {
		final RedisCommand command = patch.toCommand();
		RuntimeException failure = null;
		for (int attempt = 0; attempt < Math.max(1, this.config.getPatchAttempts()); attempt++) {
			if (attempt > 0) {
				try {
					Thread.sleep(this.config.getPatchBackoff().toMillis() << (attempt - 1));
				} catch (final InterruptedException exception) {
					Thread.currentThread().interrupt();
					break;
				}
			}

			try {
				final List<Object> reply = this.client.execute(command);
				final long status = (Long) reply.get(0);
				if (status == RedisJsonPatch.REJECTED) {
					throw new RedisLockLostException(patch.getGuard().getKey());
				}

				if (status == RedisJsonPatch.REPLAYED) {
					this.client.getMetrics().counter(this.metric("save.replayed")).increment();
				}
				return reply.size() < 2 ? null : RedisResponse.string().parse(reply.get(1));
			} catch (final RuntimeException exception) {
				if (!RedisClient.isTransient(exception)) {
					throw exception;
				}
				failure = exception;
				this.client.getMetrics().counter(this.metric("save.retry")).increment();
			}
		}

		if (this.isApplied(patch)) {
			return this.fetchJson(patch.getKey());
		}
		throw failure;
	}

	private @NonNull String metric(final @NonNull String name) {
		return this.config.getName() + "." + name;
	}

	private boolean isApplied(final @NonNull RedisJsonPatch patch) {
		try {
			final Boolean applied = this.client.execute(RedisCommand.Base.exists(patch.getMarker()));
			return applied != null && applied;
		} catch (final RuntimeException exception) {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	private @NonNull List<T> fetch(final @NonNull List<String> keys) {
		final List<T> objects = new ArrayList<>(keys.size());
		if (keys.isEmpty()) {
			return objects;
		}

		final List<RedisCommand> commands = new ArrayList<>();
		for (int i = 0; i < keys.size(); i += this.config.getBatchSize()) {
			commands.add(RedisCommand.Json.mget(RedisCommand.ROOT_PATH, keys.subList(i, Math.min(i + this.config.getBatchSize(), keys.size())).toArray(new String[0])));
		}

		for (final Object result : this.client.execute(commands.toArray(new RedisCommand[0]))) {
			for (final String json : (List<String>) result) {
				this.parse(json).ifPresent(objects::add);
			}
		}
		return objects;
	}

	private void fire(final @NonNull Consumer<SharedStoreListener<T>> action) {
		for (final SharedStoreListener<T> listener : this.listeners) {
			try {
				action.accept(listener);
			} catch (final Throwable throwable) {
				SharedStore.LOGGER.log(Level.WARNING, "Listener of store '" + this.config.getName() + "' failed", throwable);
			}
		}
	}

	private boolean isCancelled(final @NonNull Predicate<SharedStoreListener<T>> check) {
		for (final SharedStoreListener<T> listener : this.listeners) {
			if (check.test(listener)) {
				return true;
			}
		}
		return false;
	}

	private @NonNull CompletableFuture<Object> submit(final String key, final @NonNull RedisCommand command) {
		final Request request = new Request(key, command);
		if (key == null) {
			this.queue.add(request);
			return request.getFuture();
		}

		final Request existing = this.requests.putIfAbsent(key, request);
		if (existing != null) {
			this.client.getMetrics().counter(this.metric("queue.dedup.hits")).increment();
			return existing.getFuture();
		}

		this.queue.add(request);
		return request.getFuture();
	}

	private @NonNull CompletableFuture<Void> doDelete(final @NonNull T object, final @NonNull String key, final RedisLockToken token) {
		return CompletableFuture.runAsync(() -> this.client.getMetrics().timer(this.metric("delete.latency")).record(() -> {
			if (token == null) {
				this.client.execute(RedisCommand.Json.del(key));
			} else {
				final Long result = this.client.execute(RedisCommand.Base.eval(SharedStore.DELETE_SCRIPT, 2, key, token.getKey(), token.getToken()));
				if (result != null && result == RedisJsonPatch.REJECTED) {
					throw new RedisLockLostException(token.getKey());
				}
			}

			this.client.getJsonSerializer().removeSnapshot(key);
			this.fire(listener -> listener.onPostDelete(object));
		}), this.executor);
	}

	private @NonNull CompletableFuture<Void> chain(final @NonNull String key, final @NonNull Supplier<CompletableFuture<Void>> operation) {
		final CompletableFuture<Void> chained;
		try {
			chained = this.operations.compute(key, (k, previous) -> {
				final CompletableFuture<Void> base = previous == null ? CompletableFuture.completedFuture(null) : previous.handle((value, error) -> null);
				return base.thenComposeAsync(value -> operation.get(), this.executor);
			});
		} catch (final Throwable throwable) {
			return SharedStore.failed(throwable);
		}
		chained.whenComplete((result, error) -> this.operations.remove(key, chained));
		return chained;
	}

	private @NonNull CompletableFuture<Void> doSave(final @NonNull T object, final @NonNull String key, final @NonNull JsonElement captured, final RedisLockToken token) {
		return CompletableFuture.runAsync(() -> this.client.getMetrics().timer(this.metric("save.latency")).record(() -> {
			final boolean create = !this.client.getJsonSerializer().hasSnapshot(key);
			final RedisJsonSerializeResult result = this.client.getJsonSerializer().prepare(key, object, captured);
			if (token != null) {
				result.getPatch().guard(token);
			}

			final String json = this.apply(result.getPatch());
			result.getCommitLocal().run();
			this.client.getMetrics().counter(this.metric("save.success")).increment();
			this.fire(listener -> listener.onPostSave(object, json, create));
		}), this.executor);
	}

	private static Long parseVersion(final String raw) {
		if (raw == null) {
			return null;
		}

		final String value = raw.startsWith("[") ? raw.substring(1, raw.length() - 1) : raw;
		try {
			return value.isEmpty() ? null : Long.parseLong(value);
		} catch (final NumberFormatException exception) {
			return null;
		}
	}

	private static @NonNull <R> CompletableFuture<R> failed(final @NonNull Throwable throwable) {
		final CompletableFuture<R> future = new CompletableFuture<>();
		future.completeExceptionally(throwable);
		return future;
	}

	private static @NonNull ExecutorService createExecutor(final @NonNull SharedStoreConfig<?> config) {
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(config.getThreads(), config.getThreads(), 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
			final Thread thread = new Thread(runnable, "HestiaStore-" + config.getName());
			thread.setDaemon(true);
			return thread;
		}, new ThreadPoolExecutor.CallerRunsPolicy());
		executor.allowCoreThreadTimeOut(true);
		return executor;
	}

	private static @NonNull ScheduledExecutorService createScheduler(final @NonNull SharedStoreConfig<?> config) {
		return Executors.newSingleThreadScheduledExecutor(runnable -> {
			final Thread thread = new Thread(runnable, "HestiaFlusher-" + config.getName());
			thread.setDaemon(true);
			return thread;
		});
	}

	@Getter
	@RequiredArgsConstructor
	private static final class Request {

		private final String key;
		private final RedisCommand command;
		private final CompletableFuture<Object> future = new CompletableFuture<>();

		public void complete(final Object raw) {
			if (raw instanceof Throwable) {
				this.future.completeExceptionally((Throwable) raw);
				return;
			}
			this.future.complete(raw);
		}

	}

}