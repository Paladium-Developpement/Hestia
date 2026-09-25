package fr.paladium.hestia.store;

import java.lang.reflect.Field;
import java.time.Duration;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.exception.RedisLockLostException;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.impl.RedisPipeline;
import fr.paladium.hestia.redis.impl.RedisResponse;
import fr.paladium.hestia.redis.index.RedisIndexResolver;
import fr.paladium.hestia.redis.json.RedisJsonPatch;
import fr.paladium.hestia.redis.json.RedisJsonSerializeResult;
import fr.paladium.hestia.redis.json.RedisJsonSerializer;
import fr.paladium.hestia.redis.lock.RedisLockToken;
import fr.paladium.hestia.redis.metric.RedisMetricRecorder;
import fr.paladium.hestia.redis.query.RedisQuery;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import redis.clients.jedis.exceptions.JedisDataException;

public class RedisStore<T> implements AutoCloseable {

	private static final Logger LOGGER = Logger.getLogger(RedisStore.class.getName());
	private static final String DELETE_SCRIPT = "if redis.call('GET', KEYS[2]) ~= ARGV[1] then return -1 end return redis.call('DEL', KEYS[1])";

	private final Batcher reads;
	private final String prefix;
	private final Batcher writes;
	private final boolean snapshotted;
	private final ExecutorService executor;
	@Getter private final String versionPath;
	@Getter private final RedisClient client;
	@Getter private final RedisStoreConfig<T> config;

	private final Map<String, Request> requests = new ConcurrentHashMap<>();
	private final List<RedisStoreListener<T>> listeners = new CopyOnWriteArrayList<>();
	private final Map<String, CompletableFuture<Void>> operations = new ConcurrentHashMap<>();

	private RedisStore(final @NonNull RedisClient client, final @NonNull RedisStoreConfig<T> config) {
		final Field versionField = RedisJsonSerializer.resolveVersionField(config.getType());
		this.client = client;
		this.config = config;
		this.prefix = config.getName() + ":";
		this.versionPath = versionField == null ? null : RedisCommand.ROOT_PATH + "." + versionField.getName();
		this.executor = RedisStore.createExecutor(config);
		this.snapshotted = RedisJsonSerializer.resolveSnapshotField(config.getType()) != null;
		this.reads = new Batcher("read", config.getReadFlushInterval(), false);
		this.writes = new Batcher("write", config.getWriteFlushInterval(), true);
	}

	public static @NonNull <T> RedisStore<T> create(final @NonNull RedisClient client, final @NonNull RedisStoreConfig<T> config) {
		return new RedisStore<>(client, config);
	}

	@Override
	public void close() {
		this.reads.close();
		this.writes.close();
		this.executor.shutdown();
		try {
			this.executor.awaitTermination(30L, TimeUnit.SECONDS);
		} catch (final InterruptedException exception) {
			Thread.currentThread().interrupt();
		}
	}

	public void flush() {
		this.reads.drainNow();
		this.writes.drainNow();
	}

	public boolean index() {
		return this.client.index(this.config.getType());
	}

	public long getVersion(final @NonNull T object) {
		return this.client.getJsonSerializer().getVersion(object);
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

		for (final RedisStoreListener<T> listener : this.listeners) {
			listener.onPostLoad(object);
		}

		this.client.getJsonSerializer().bind(object, tree);
		this.client.getJsonSerializer().snapshot(this.getKey(this.getId(object)), tree, this.config.getType());
		return Optional.of(object);
	}

	public @NonNull CompletableFuture<List<T>> fetchAll() {
		this.client.getMetrics().counter(this.metric("fetchAll.total")).increment();
		return CompletableFuture.supplyAsync(() -> this.client.getMetrics().timer(this.metric("fetchAll.latency")).record(() -> this.fetch(this.scanKeys())), this.executor);
	}

	public @NonNull String getId(final @NonNull T object) {
		return this.config.getIdentifier().apply(object);
	}

	public @NonNull String getKey(final @NonNull String id) {
		return this.prefix + id;
	}

	public @NonNull CompletableFuture<T> fetch(final @NonNull String id) {
		this.client.getMetrics().counter(this.metric("fetch.total")).increment();
		return this.<String>queue(this.prefix + "fetch:" + id, RedisCommand.Json.get(this.getKey(id))).thenApply(json -> this.parse(json).orElse(null));
	}

	public @NonNull CompletableFuture<Map<String, Long>> fetchVersions() {
		final String path = this.requireVersionPath();
		this.client.getMetrics().counter(this.metric("fetchVersions.total")).increment();
		return CompletableFuture.supplyAsync(() -> this.client.getMetrics().timer(this.metric("fetchVersions.latency")).record(() -> {
			final List<String> keys = this.scanKeys();
			final Map<String, Long> versions = new HashMap<>(keys.size());
			if (keys.isEmpty()) {
				return versions;
			}

			final List<String> values = this.client.execute(RedisCommand.Json.mget(path, keys.toArray(new String[0])).withResponse(RedisResponse.stringList()));
			for (int i = 0; i < keys.size(); i++) {
				final Long version = RedisStore.parseVersion(values.get(i));
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

	public @NonNull CompletableFuture<T> find(final @NonNull String query) {
		this.client.getMetrics().counter(this.metric("find.total")).increment();
		final RedisCommand command = RedisCommand.Search.search(RedisIndexResolver.indexName(this.config.getType()), query);
		return this.<String>queue(this.prefix + "find:" + query, command).thenApply(json -> this.parse(json).orElse(null));
	}

	public @NonNull CompletableFuture<Void> delete(final @NonNull T object) {
		return this.delete(object, null);
	}

	public @NonNull CompletableFuture<Long> fetchVersion(final @NonNull String id) {
		final RedisCommand command = RedisCommand.Json.get(this.getKey(id), this.requireVersionPath());
		return this.<String>queue(this.prefix + "version:" + id, command).thenApply(RedisStore::parseVersion);
	}

	public @NonNull <R> CompletableFuture<R> queue(final @NonNull RedisQuery<R> query) {
		return this.queue(query.cacheKey(), query.toCommand(this.client.getJsonSerializer().getGson()));
	}

	public @NonNull <R> CompletableFuture<R> queue(final @NonNull RedisCommand command) {
		return this.submit(null, command).thenApply(command::parseResponse);
	}

	public @NonNull RedisStore<T> listen(final @NonNull RedisStoreListener<T> listener) {
		this.listeners.add(listener);
		return this;
	}

	public @NonNull CompletableFuture<List<T>> fetchAll(final @NonNull Collection<String> ids) {
		final List<String> keys = new ArrayList<>(ids.size());
		for (final String id : ids) {
			keys.add(this.getKey(id));
		}
		return CompletableFuture.supplyAsync(() -> this.fetch(keys), this.executor);
	}

	public @NonNull CompletableFuture<Void> save(final @NonNull T object, final RedisLockToken token) {
		final String key = this.getKey(this.getId(object));
		final boolean create = !this.client.getJsonSerializer().hasSnapshot(key);
		final JsonElement captured;
		try {
			if (this.isCancelled(listener -> listener.onPreSave(object, create))) {
				return CompletableFuture.completedFuture(null);
			}
			captured = this.client.getJsonSerializer().capture(object);
		} catch (final Throwable throwable) {
			return RedisStore.failed(throwable);
		}

		this.client.getMetrics().counter(this.metric("save.total")).increment();
		return this.chain(this.lane(key, object), () -> this.doSave(object, key, captured, token));
	}

	public @NonNull CompletableFuture<Void> delete(final @NonNull T object, final RedisLockToken token) {
		try {
			if (this.isCancelled(listener -> listener.onPreDelete(object))) {
				return CompletableFuture.completedFuture(null);
			}
		} catch (final Throwable throwable) {
			return RedisStore.failed(throwable);
		}

		this.client.getMetrics().counter(this.metric("delete.total")).increment();
		final String key = this.getKey(this.getId(object));
		return this.chain(this.lane(key, object), () -> this.doDelete(object, key, token));
	}

	public @NonNull <R> CompletableFuture<R> queue(final String key, final @NonNull RedisCommand command) {
		return this.submit(key, command).thenApply(command::parseResponse);
	}

	private boolean isMergedRequired() {
		for (final RedisStoreListener<T> listener : this.listeners) {
			if (listener.requiresMergedDocument()) {
				return true;
			}
		}
		return false;
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

	private Object write(final @NonNull RedisCommand command) {
		final Request request = new Request(null, command);
		this.writes.submit(request);
		try {
			return request.getFuture().get();
		} catch (final InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(exception);
		} catch (final ExecutionException exception) {
			if (exception.getCause() instanceof RuntimeException) {
				throw (RuntimeException) exception.getCause();
			}
			throw new IllegalStateException(exception.getCause());
		}
	}

	private String apply(final @NonNull RedisJsonPatch patch) {
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
				final List<Object> reply = this.send(patch);
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
			return patch.isMerged() ? this.fetchJson(patch.getKey()) : null;
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

	@SuppressWarnings("unchecked")
	private @NonNull List<Object> send(final @NonNull RedisJsonPatch patch) {
		try {
			return (List<Object>) this.write(patch.toShaCommand());
		} catch (final JedisDataException exception) {
			if (exception.getMessage() == null || !exception.getMessage().startsWith("NOSCRIPT")) {
				throw exception;
			}
			return (List<Object>) this.write(patch.toCommand());
		}
	}

	private void fire(final @NonNull Consumer<RedisStoreListener<T>> action) {
		for (final RedisStoreListener<T> listener : this.listeners) {
			try {
				action.accept(listener);
			} catch (final Throwable throwable) {
				RedisStore.LOGGER.log(Level.WARNING, "Listener of store '" + this.config.getName() + "' failed", throwable);
			}
		}
	}

	private @NonNull String lane(final @NonNull String key, final @NonNull T object) {
		return this.snapshotted ? key + "@" + Integer.toHexString(System.identityHashCode(object)) : key;
	}

	private boolean isCancelled(final @NonNull Predicate<RedisStoreListener<T>> check) {
		for (final RedisStoreListener<T> listener : this.listeners) {
			if (check.test(listener)) {
				return true;
			}
		}
		return false;
	}

	private void doDelete(final @NonNull T object, final @NonNull String key, final RedisLockToken token) {
		this.client.getMetrics().timer(this.metric("delete.latency")).record(() -> {
			if (token == null) {
				this.write(RedisCommand.Json.del(key));
			} else {
				final Long result = (Long) this.write(RedisCommand.Base.eval(RedisStore.DELETE_SCRIPT, 2, key, token.getKey(), token.getToken()));
				if (result != null && result == RedisJsonPatch.REJECTED) {
					throw new RedisLockLostException(token.getKey());
				}
			}

			this.client.getJsonSerializer().removeSnapshot(key);
			this.fire(listener -> listener.onPostDelete(object));
		});
	}

	private @NonNull CompletableFuture<Object> submit(final String key, final @NonNull RedisCommand command) {
		final Request request = new Request(key, command);
		if (key == null) {
			this.reads.submit(request);
			return request.getFuture();
		}

		final Request existing = this.requests.putIfAbsent(key, request);
		if (existing != null) {
			this.client.getMetrics().counter(this.metric("queue.dedup.hits")).increment();
			return existing.getFuture();
		}

		this.reads.submit(request);
		return request.getFuture();
	}

	private @NonNull CompletableFuture<Void> chain(final @NonNull String lane, final @NonNull Runnable operation) {
		final CompletableFuture<Void> chained;
		try {
			chained = this.operations.compute(lane, (k, previous) -> previous == null ? CompletableFuture.runAsync(operation, this.executor) : previous.handle((value, error) -> null).thenRunAsync(operation, this.executor));
		} catch (final Throwable throwable) {
			return RedisStore.failed(throwable);
		}
		chained.whenComplete((result, error) -> this.operations.remove(lane, chained));
		return chained;
	}

	private void doSave(final @NonNull T object, final @NonNull String key, final @NonNull JsonElement captured, final RedisLockToken token) {
		this.client.getMetrics().timer(this.metric("save.latency")).record(() -> {
			final boolean create = !this.client.getJsonSerializer().hasSnapshot(key);
			final RedisJsonSerializeResult result = this.client.getJsonSerializer().prepare(key, object, captured);
			if (token != null) {
				result.getPatch().guard(token);
			}

			result.getPatch().merged(this.isMergedRequired());
			final String json = this.apply(result.getPatch());
			result.getCommitLocal().run();
			this.client.getMetrics().counter(this.metric("save.success")).increment();
			this.fire(listener -> listener.onPostSave(object, json, create));
		});
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

	private static @NonNull ExecutorService createExecutor(final @NonNull RedisStoreConfig<?> config) {
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(config.getThreads(), config.getThreads(), 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
			final Thread thread = new Thread(runnable, "HestiaStore-" + config.getName());
			thread.setDaemon(true);
			return thread;
		}, new ThreadPoolExecutor.CallerRunsPolicy());
		executor.allowCoreThreadTimeOut(true);
		return executor;
	}

	private final class Batcher {

		private final int drains;
		private final String name;
		private final long lingerMs;
		private final boolean inline;
		private final ScheduledThreadPoolExecutor threads;
		private final AtomicInteger active = new AtomicInteger();
		private final AtomicBoolean scheduled = new AtomicBoolean();
		private final Queue<Request> queue = new ConcurrentLinkedQueue<>();

		private Batcher(final @NonNull String name, final @NonNull Duration linger, final boolean inline) {
			this.name = name;
			this.inline = inline;
			this.lingerMs = Math.max(0L, linger.toMillis());
			this.drains = Math.max(1, RedisStore.this.client.getConfig().getPoolSize());
			this.threads = new ScheduledThreadPoolExecutor(this.drains, runnable -> {
				final Thread flusher = new Thread(runnable, "HestiaFlusher-" + RedisStore.this.config.getName() + "-" + name);
				flusher.setDaemon(true);
				return flusher;
			});
			this.threads.setKeepAliveTime(60L, TimeUnit.SECONDS);
			this.threads.allowCoreThreadTimeOut(true);
		}

		public void close() {
			this.threads.shutdown();
			try {
				this.threads.awaitTermination(30L, TimeUnit.SECONDS);
			} catch (final InterruptedException exception) {
				Thread.currentThread().interrupt();
			}

			List<Request> batch;
			while (!(batch = this.poll()).isEmpty()) {
				this.send(batch);
			}
		}

		public void drainNow() {
			this.spawn();
		}

		public void submit(final @NonNull Request request) {
			this.queue.add(request);
			if (this.inline && this.lingerMs <= 0L && this.acquire()) {
				this.drain();
				return;
			}
			this.wake();
		}

		private void wake() {
			if (this.lingerMs <= 0L || this.threads.isShutdown()) {
				this.spawn();
			} else if (this.scheduled.compareAndSet(false, true)) {
				this.threads.schedule(this::release, this.lingerMs, TimeUnit.MILLISECONDS);
			}
		}

		private void drain() {
			try {
				final List<Request> batch = this.poll();
				if (!batch.isEmpty()) {
					this.send(batch);
				}
			} finally {
				this.active.decrementAndGet();
			}

			if (!this.queue.isEmpty()) {
				this.wake();
			}
		}

		private void spawn() {
			if (this.queue.isEmpty() || !this.acquire()) {
				return;
			}

			if (this.threads.isShutdown()) {
				this.drain();
			} else {
				this.threads.execute(this::drain);
			}
		}

		private void release() {
			this.scheduled.set(false);
			this.spawn();
		}

		private boolean acquire() {
			while (true) {
				final int current = this.active.get();
				if (current >= this.drains) {
					return false;
				}

				if (this.active.compareAndSet(current, current + 1)) {
					return true;
				}
			}
		}

		private @NonNull List<Request> poll() {
			final List<Request> batch = new ArrayList<>();
			Request request;
			while ((request = this.queue.poll()) != null) {
				batch.add(request);
				if (request.getKey() != null) {
					RedisStore.this.requests.remove(request.getKey(), request);
				}
			}
			return batch;
		}

		private void send(final @NonNull List<Request> batch) {
			final RedisMetricRecorder metrics = RedisStore.this.client.getMetrics();
			metrics.gauge(RedisStore.this.metric(this.name + ".flush.size")).set(batch.size());
			metrics.counter(RedisStore.this.metric(this.name + ".flush.total")).increment();
			try {
				metrics.timer(RedisStore.this.metric(this.name + ".flush.latency")).record(() -> this.execute(batch));
			} catch (final Throwable throwable) {
				metrics.counter(RedisStore.this.metric(this.name + ".flush.failed")).increment();
				for (final Request failed : batch) {
					failed.getFuture().completeExceptionally(throwable);
				}
			}
		}

		private void execute(final @NonNull List<Request> batch) {
			if (batch.size() == 1) {
				this.executeSingle(batch.get(0));
				return;
			}

			final RedisPipeline pipeline = RedisStore.this.client.pipeline();
			for (final Request request : batch) {
				pipeline.add(request.getCommand());
			}

			final List<Object> results = pipeline.execute();
			for (int i = 0; i < batch.size(); i++) {
				this.complete(batch.get(i), results.get(i));
			}
		}

		private void executeSingle(final @NonNull Request request) {
			Object result;
			try {
				result = RedisStore.this.client.execute(request.getCommand().withResponse(RedisResponse.raw()));
			} catch (final RuntimeException exception) {
				result = exception;
			}
			this.complete(request, result);
		}

		private void complete(final @NonNull Request request, final Object result) {
			if (this.inline) {
				request.complete(result);
			} else {
				RedisStore.this.dispatch(() -> request.complete(result));
			}
		}

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