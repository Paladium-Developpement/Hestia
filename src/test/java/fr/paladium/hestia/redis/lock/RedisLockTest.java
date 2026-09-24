package fr.paladium.hestia.redis.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.paladium.hestia.HestiaTestSupport;
import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.exception.RedisLockBusyException;
import fr.paladium.hestia.redis.impl.RedisCommand;

public class RedisLockTest {

	private static RedisClient client;

	@AfterAll
	public static void disconnect() {
		RedisLockTest.client.close();
	}

	@BeforeAll
	public static void connect() throws Exception {
		RedisLockTest.client = HestiaTestSupport.client();
	}

	@Test
	public void lockIsReleasedAfterward() throws Exception {
		final String key = "lock:" + HestiaTestSupport.randomId();
		HestiaTestSupport.join(RedisLockTest.client.getLock().withLock(key, token -> CompletableFuture.completedFuture(token.getToken())));
		assertFalse(RedisLockTest.client.<Boolean>execute(RedisCommand.Base.exists(key)));
	}

	@Test
	public void busyLocksFailWithoutWaiting() throws Exception {
		final String key = "lock:" + HestiaTestSupport.randomId();
		final CompletableFuture<Void> release = new CompletableFuture<>();
		final CompletableFuture<Void> holder = RedisLockTest.client.getLock().withLock(key, token -> release);
		HestiaTestSupport.await(() -> RedisLockTest.client.<Boolean>execute(RedisCommand.Base.exists(key)));

		final ExecutionException error = assertThrows(ExecutionException.class, () -> HestiaTestSupport.join(RedisLockTest.client.getLock().withLock(key, token -> CompletableFuture.completedFuture(null))));
		release.complete(null);
		HestiaTestSupport.join(holder);
		assertInstanceOf(RedisLockBusyException.class, error.getCause());
	}

	@Test
	public void criticalSectionsNeverOverlap() throws Exception {
		final String key = "lock:" + HestiaTestSupport.randomId();
		final AtomicInteger inside = new AtomicInteger();
		final AtomicInteger overlaps = new AtomicInteger();
		final List<CompletableFuture<Void>> futures = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			futures.add(RedisLockTest.client.getLock().withLockWaiting(key, token -> CompletableFuture.runAsync(() -> {
				if (inside.incrementAndGet() > 1) {
					overlaps.incrementAndGet();
				}

				try {
					Thread.sleep(5L);
				} catch (final InterruptedException exception) {
					Thread.currentThread().interrupt();
				}
				inside.decrementAndGet();
			})));
		}

		HestiaTestSupport.join(CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])));
		assertEquals(0, overlaps.get());
	}

}