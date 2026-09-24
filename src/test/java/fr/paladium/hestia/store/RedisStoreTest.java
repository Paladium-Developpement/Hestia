package fr.paladium.hestia.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import fr.paladium.hestia.HestiaTestSupport;
import fr.paladium.hestia.model.TestAccount;
import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.exception.RedisLockLostException;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.lock.RedisLockToken;
import fr.paladium.hestia.redis.query.RedisQuery;
import redis.clients.jedis.Jedis;

@Tag("integration")
public class RedisStoreTest {

	private static RedisClient firstClient;
	private static RedisClient secondClient;
	private static RedisStore<TestAccount> first;
	private static RedisStore<TestAccount> second;

	@AfterAll
	public static void disconnect() {
		RedisStoreTest.first.close();
		RedisStoreTest.second.close();
		RedisStoreTest.firstClient.close();
		RedisStoreTest.secondClient.close();
	}

	@BeforeAll
	public static void connect() throws Exception {
		RedisStoreTest.firstClient = HestiaTestSupport.client();
		RedisStoreTest.secondClient = HestiaTestSupport.client();
		RedisStoreTest.first = RedisStore.create(RedisStoreTest.firstClient, RedisStoreConfig.create(TestAccount.class, TestAccount::getId));
		RedisStoreTest.second = RedisStore.create(RedisStoreTest.secondClient, RedisStoreConfig.create(TestAccount.class, TestAccount::getId));
		RedisStoreTest.first.index();
	}

	@Test
	public void everyObjectIsListed() throws Exception {
		final List<String> ids = new ArrayList<>();
		for (int i = 0; i < 250; i++) {
			ids.add(RedisStoreTest.account(i).getId());
		}

		final List<String> listed = new ArrayList<>();
		for (final TestAccount account : HestiaTestSupport.join(RedisStoreTest.second.fetchAll())) {
			listed.add(account.getId());
		}
		assertTrue(listed.containsAll(ids));
		assertEquals(250, HestiaTestSupport.join(RedisStoreTest.second.fetchAll(ids)).size());
	}

	@Test
	public void savedObjectsCanBeRead() throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setBalance(100L);
		account.getTags().add("founder");
		HestiaTestSupport.join(RedisStoreTest.first.save(account));

		final TestAccount read = HestiaTestSupport.join(RedisStoreTest.second.fetch(account.getId()));
		assertEquals(100L, read.getBalance());
		assertEquals(1L, read.getVersion());
		assertEquals(Arrays.asList("founder"), read.getTags());
	}

	@Test
	public void missingObjectsAreNull() throws Exception {
		assertNull(HestiaTestSupport.join(RedisStoreTest.first.fetch(HestiaTestSupport.randomId())));
	}

	@Test
	public void nonObjectsCannotBeSaved() throws Exception {
		final RedisStore<String> store = RedisStore.create(RedisStoreTest.firstClient, RedisStoreConfig.create(String.class, value -> value));
		final ExecutionException error = assertThrows(ExecutionException.class, () -> HestiaTestSupport.join(store.save("text")));
		store.close();
		assertInstanceOf(IllegalArgumentException.class, error.getCause());
	}

	@Test
	public void deletedObjectsDisappear() throws Exception {
		final TestAccount account = RedisStoreTest.account(1L);
		HestiaTestSupport.join(RedisStoreTest.first.delete(account));
		assertNull(HestiaTestSupport.join(RedisStoreTest.second.fetch(account.getId())));
	}

	@Test
	public void listenersCanCancelSaves() throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		final RedisStoreListener<TestAccount> listener = new RedisStoreListener<TestAccount>() {

			@Override
			public boolean onPreSave(final TestAccount object, final boolean create) {
				return object.getId().equals(account.getId());
			}

		};

		RedisStoreTest.first.listen(listener);
		HestiaTestSupport.join(RedisStoreTest.first.save(account));
		assertNull(HestiaTestSupport.join(RedisStoreTest.first.fetch(account.getId())));
	}

	@Test
	public void lostLocksRejectTheWrite() throws Exception {
		final TestAccount account = RedisStoreTest.account(10L);
		account.setBalance(20L);

		final ExecutionException error = assertThrows(ExecutionException.class, () -> HestiaTestSupport.join(RedisStoreTest.first.save(account, new RedisLockToken("lock:" + account.getId(), "expired"))));
		assertInstanceOf(RedisLockLostException.class, error.getCause());
		assertEquals(10L, HestiaTestSupport.join(RedisStoreTest.second.fetch(account.getId())).getBalance());
	}

	@Test
	public void versionsFollowEverySave() throws Exception {
		final TestAccount account = RedisStoreTest.account(0L);
		account.setBalance(1L);
		HestiaTestSupport.join(RedisStoreTest.first.save(account));

		final Map<String, Long> versions = HestiaTestSupport.join(RedisStoreTest.second.fetchVersions());
		assertEquals(2L, versions.get(account.getId()));
		assertEquals(2L, HestiaTestSupport.join(RedisStoreTest.second.fetchVersion(account.getId())));
	}

	@Test
	public void lostLocksRejectTheDelete() throws Exception {
		final TestAccount account = RedisStoreTest.account(4L);
		final ExecutionException error = assertThrows(ExecutionException.class, () -> HestiaTestSupport.join(RedisStoreTest.first.delete(account, new RedisLockToken("lock:" + account.getId(), "expired"))));
		assertInstanceOf(RedisLockLostException.class, error.getCause());
		assertNotNull(HestiaTestSupport.join(RedisStoreTest.second.fetch(account.getId())));
	}

	@Test
	public void listenersCanCancelDeletes() throws Exception {
		final TestAccount account = RedisStoreTest.account(3L);
		RedisStoreTest.first.listen(new RedisStoreListener<TestAccount>() {

			@Override
			public boolean onPreDelete(final TestAccount object) {
				return object.getId().equals(account.getId());
			}

		});

		HestiaTestSupport.join(RedisStoreTest.first.delete(account));
		assertNotNull(HestiaTestSupport.join(RedisStoreTest.second.fetch(account.getId())));
	}

	@Test
	public void queriesAreQueuedLikeReads() throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setOwner("query" + account.getId());
		HestiaTestSupport.join(RedisStoreTest.first.save(account));

		final RedisQuery<List<TestAccount>> query = RedisQuery.list(TestAccount.class).tag("owner", account.getOwner());
		assertTrue(HestiaTestSupport.await(() -> RedisStoreTest.first.queue(query).join().size() == 1));
		assertEquals(account.getId(), HestiaTestSupport.join(RedisStoreTest.first.queue(query)).get(0).getId());
	}

	@Test
	public void indexedFieldsCanBeSearched() throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setOwner("owner" + account.getId());
		HestiaTestSupport.join(RedisStoreTest.first.save(account));

		assertTrue(HestiaTestSupport.await(() -> RedisStoreTest.first.find("@owner:{" + account.getOwner() + "}").join() != null));
		assertEquals(account.getId(), HestiaTestSupport.join(RedisStoreTest.first.find("@owner:{" + account.getOwner() + "}")).getId());
	}

	@Test
	public void heldLocksAllowGuardedWrites() throws Exception {
		final TestAccount account = RedisStoreTest.account(10L);
		HestiaTestSupport.join(RedisStoreTest.firstClient.getLock().withLock("lock:" + account.getId(), token -> {
			account.setBalance(30L);
			return RedisStoreTest.first.save(account, token);
		}));
		assertEquals(30L, HestiaTestSupport.join(RedisStoreTest.second.fetch(account.getId())).getBalance());
	}

	@Test
	public void loadedObjectsReachTheListeners() throws Exception {
		final TestAccount account = RedisStoreTest.account(6L);
		final List<String> loaded = new CopyOnWriteArrayList<>();
		RedisStoreTest.second.listen(new RedisStoreListener<TestAccount>() {

			@Override
			public void onPostLoad(final TestAccount object) {
				loaded.add(object.getId());
			}

		});

		HestiaTestSupport.join(RedisStoreTest.second.fetch(account.getId()));
		assertTrue(loaded.contains(account.getId()));
	}

	@Test
	public void foreignKeysAreIgnoredByTheScan() throws Exception {
		final String foreign = "testaccount:lock:" + HestiaTestSupport.randomId();
		try (Jedis admin = HestiaTestSupport.admin()) {
			admin.set(foreign, "1");
		}
		assertFalse(HestiaTestSupport.join(RedisStoreTest.first.fetchVersions()).containsKey(foreign.substring("testaccount:".length())));
	}

	@Test
	public void concurrentReadsGetTheirOwnInstance() throws Exception {
		final TestAccount account = RedisStoreTest.account(5L);
		final CompletableFuture<TestAccount> firstRead = RedisStoreTest.first.fetch(account.getId());
		final CompletableFuture<TestAccount> secondRead = RedisStoreTest.first.fetch(account.getId());
		assertNotSame(HestiaTestSupport.join(firstRead), HestiaTestSupport.join(secondRead));
	}

	@Test
	public void concurrentServersMergeTheirChanges() throws Exception {
		final TestAccount account = RedisStoreTest.account(1000L);
		final TestAccount onFirst = HestiaTestSupport.join(RedisStoreTest.first.fetch(account.getId()));
		final TestAccount onSecond = HestiaTestSupport.join(RedisStoreTest.second.fetch(account.getId()));
		onFirst.setBalance(onFirst.getBalance() + 100L);
		onFirst.getTags().add("first");
		onFirst.setLastSeen(111L);
		onSecond.setBalance(onSecond.getBalance() - 30L);
		onSecond.getTags().add("second");
		onSecond.setLastSeen(222L);

		HestiaTestSupport.join(RedisStoreTest.first.save(onFirst));
		HestiaTestSupport.join(RedisStoreTest.second.save(onSecond));

		final TestAccount merged = HestiaTestSupport.join(RedisStoreTest.first.fetch(account.getId()));
		assertEquals(1070L, merged.getBalance());
		assertEquals(222L, merged.getLastSeen());
		assertEquals(3L, merged.getVersion());
		assertEquals(new HashSet<>(Arrays.asList("first", "second")), new HashSet<>(merged.getTags()));
	}

	@Test
	public void parallelWritersNeverLoseAnIncrement() throws Exception {
		final TestAccount account = RedisStoreTest.account(0L);
		final ExecutorService pool = Executors.newFixedThreadPool(8);
		final List<Future<?>> tasks = new ArrayList<>();
		for (int thread = 0; thread < 8; thread++) {
			final RedisStore<TestAccount> store = thread % 2 == 0 ? RedisStoreTest.first : RedisStoreTest.second;
			tasks.add(pool.submit(() -> {
				for (int i = 0; i < 25; i++) {
					final TestAccount copy = HestiaTestSupport.join(store.fetch(account.getId()));
					copy.setBalance(copy.getBalance() + 1L);
					HestiaTestSupport.join(store.save(copy));
				}
				return null;
			}));
		}

		for (final Future<?> task : tasks) {
			task.get();
		}
		pool.shutdown();

		final TestAccount stored = HestiaTestSupport.join(RedisStoreTest.first.fetch(account.getId()));
		assertNotNull(stored);
		assertEquals(200L, stored.getBalance());
		assertEquals(201L, stored.getVersion());
	}

	@Test
	public void transientFieldsAreReadButNeverStored() throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setDescription("derived");
		HestiaTestSupport.join(RedisStoreTest.first.save(account));

		final String key = RedisStoreTest.first.getKey(account.getId());
		assertFalse(RedisStoreTest.firstClient.<String>execute(RedisCommand.Json.get(key)).contains("description"));

		RedisStoreTest.firstClient.execute(RedisCommand.Json.set(new Gson(), key, account));
		assertEquals("derived", HestiaTestSupport.join(RedisStoreTest.second.fetch(account.getId())).getDescription());
	}

	private static TestAccount account(final long balance) throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setBalance(balance);
		HestiaTestSupport.join(RedisStoreTest.first.save(account));
		return account;
	}

}