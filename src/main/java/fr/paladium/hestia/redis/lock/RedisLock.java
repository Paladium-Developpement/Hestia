package fr.paladium.hestia.redis.lock;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.impl.RedisCommand;
import lombok.NonNull;

public final class RedisLock implements AutoCloseable {

	private static final int WATCHDOG_THREADS = 4;
	private static final long DEFAULT_TTL_MS = 5000L;
	private static final long ACQUIRE_RETRY_MS = 25L;
	private static final long ACQUIRE_WAIT_MS = 60000L;
	private static final long ACQUIRE_RETRY_MAX_MS = 500L;

	private static final String RELEASE_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
	private static final String RENEW_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end";

	private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();
	private static final AtomicInteger WATCHDOG_COUNTER = new AtomicInteger();

	private final RedisClient client;
	private final ExecutorService executor;
	private final ScheduledExecutorService watchdog;

	private RedisLock(final @NonNull RedisClient client) {
		this.client = client;
		this.executor = RedisLock.createExecutor();
		this.watchdog = RedisLock.createWatchdog();
	}

	public static @NonNull RedisLock create(final @NonNull RedisClient client) {
		return new RedisLock(client);
	}

	@Override
	public void close() {
		this.watchdog.shutdownNow();
		this.executor.shutdown();
	}

	public @NonNull <T> CompletableFuture<T> withLock(final @NonNull String key, final @NonNull Function<RedisLockToken, CompletableFuture<T>> critical) {
		return this.withLock(key, RedisLock.DEFAULT_TTL_MS, 0L, critical);
	}

	public @NonNull <T> CompletableFuture<T> withLockWaiting(final @NonNull String key, final @NonNull Function<RedisLockToken, CompletableFuture<T>> critical) {
		return this.withLock(key, RedisLock.DEFAULT_TTL_MS, RedisLock.ACQUIRE_WAIT_MS, critical);
	}

	public @NonNull <T> CompletableFuture<T> withLock(final @NonNull String key, final long ttlMs, final @NonNull Function<RedisLockToken, CompletableFuture<T>> critical) {
		return this.withLock(key, ttlMs, 0L, critical);
	}

	public @NonNull <T> CompletableFuture<T> withLock(final @NonNull String key, final long ttlMs, final long waitMs, final @NonNull Function<RedisLockToken, CompletableFuture<T>> critical) {
		return this.acquire(key, ttlMs, System.currentTimeMillis() + waitMs).thenCompose(token -> {
			final ScheduledFuture<?> renewal = this.watchdog.scheduleAtFixedRate(() -> this.renew(token, ttlMs), ttlMs / 3L, ttlMs / 3L, TimeUnit.MILLISECONDS);
			CompletableFuture<T> result;

			try {
				result = critical.apply(token);
			} catch (final Throwable throwable) {
				result = new CompletableFuture<>();
				result.completeExceptionally(throwable);
			}

			return result.whenCompleteAsync((value, error) -> {
				renewal.cancel(false);
				this.release(token);
			}, this.executor);
		});
	}

	private void release(final @NonNull RedisLockToken token) {
		try {
			final Long deleted = this.client.execute(RedisCommand.Base.eval(RedisLock.RELEASE_SCRIPT, 1, token.getKey(), token.getToken()));
			if (deleted == null || deleted == 0L) {
				this.client.getMetrics().counter("redis.lock.lost").increment();
			}
		} catch (final Throwable ignored) {}
	}

	private void renew(final @NonNull RedisLockToken token, final long ttlMs) {
		try {
			this.client.execute(RedisCommand.Base.eval(RedisLock.RENEW_SCRIPT, 1, token.getKey(), token.getToken(), String.valueOf(ttlMs)));
		} catch (final Throwable ignored) {}
	}

	private @NonNull CompletableFuture<RedisLockToken> acquire(final @NonNull String key, final long ttlMs, final long deadline) {
		final RedisLockToken token = new RedisLockToken(key, UUID.randomUUID().toString());
		final CompletableFuture<RedisLockToken> future = new CompletableFuture<>();
		this.submit(token, ttlMs, deadline, RedisLock.ACQUIRE_RETRY_MS, future);
		return future;
	}

	private void submit(final @NonNull RedisLockToken token, final long ttlMs, final long deadline, final long retryMs, final @NonNull CompletableFuture<RedisLockToken> future) {
		try {
			this.executor.execute(() -> this.tryAcquire(token, ttlMs, deadline, retryMs, future));
		} catch (final Throwable throwable) {
			future.completeExceptionally(throwable);
		}
	}

	private void tryAcquire(final @NonNull RedisLockToken token, final long ttlMs, final long deadline, final long retryMs, final @NonNull CompletableFuture<RedisLockToken> future) {
		try {
			if ("OK".equals(this.client.execute(RedisCommand.Base.setNxPx(token.getKey(), token.getToken(), ttlMs)))) {
				future.complete(token);
				return;
			}

			if (System.currentTimeMillis() >= deadline) {
				future.completeExceptionally(new RedisLockBusyException(token.getKey()));
				return;
			}

			this.watchdog.schedule(() -> this.submit(token, ttlMs, deadline, Math.min(retryMs * 2L, RedisLock.ACQUIRE_RETRY_MAX_MS), future), retryMs, TimeUnit.MILLISECONDS);
		} catch (final Throwable throwable) {
			future.completeExceptionally(throwable);
		}
	}

	private static @NonNull ExecutorService createExecutor() {
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
			final Thread thread = new Thread(runnable, "HestiaLockWorker-" + RedisLock.THREAD_COUNTER.incrementAndGet());
			thread.setDaemon(true);
			return thread;
		}, new ThreadPoolExecutor.CallerRunsPolicy());
		executor.allowCoreThreadTimeOut(true);
		return executor;
	}

	private static @NonNull ScheduledExecutorService createWatchdog() {
		return Executors.newScheduledThreadPool(RedisLock.WATCHDOG_THREADS, runnable -> {
			final Thread thread = new Thread(runnable, "HestiaLockWatchdog-" + RedisLock.WATCHDOG_COUNTER.incrementAndGet());
			thread.setDaemon(true);
			return thread;
		});
	}

}