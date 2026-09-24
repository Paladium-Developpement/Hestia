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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import fr.paladium.hestia.HestiaTestSupport;
import fr.paladium.hestia.model.TestAccount;
import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.lock.RedisLockLostException;
import fr.paladium.hestia.redis.lock.RedisLockToken;
import redis.clients.jedis.Jedis;

public class SharedStoreTest {

	private static RedisClient firstClient;
	private static RedisClient secondClient;
	private static SharedStore<TestAccount> first;
	private static SharedStore<TestAccount> second;

	@AfterAll
	public static void disconnect() {
		SharedStoreTest.first.close();
		SharedStoreTest.second.close();
		SharedStoreTest.firstClient.close();
		SharedStoreTest.secondClient.close();
	}

	@BeforeAll
	public static void connect() throws Exception {
		SharedStoreTest.firstClient = HestiaTestSupport.client();
		SharedStoreTest.secondClient = HestiaTestSupport.client();
		SharedStoreTest.first = SharedStore.create(SharedStoreTest.firstClient, SharedStoreConfig.create(TestAccount.class, TestAccount::getId));
		SharedStoreTest.second = SharedStore.create(SharedStoreTest.secondClient, SharedStoreConfig.create(TestAccount.class, TestAccount::getId));
		SharedStoreTest.first.index();
	}

	@Test
	public void everyObjectIsListed() throws Exception {
		final List<String> ids = new ArrayList<>();
		for (int i = 0; i < 250; i++) {
			ids.add(SharedStoreTest.account(i).getId());
		}

		final List<String> listed = new ArrayList<>();
		for (final TestAccount account : HestiaTestSupport.join(SharedStoreTest.second.getAll())) {
			listed.add(account.getId());
		}
		assertTrue(listed.containsAll(ids));
		assertEquals(250, HestiaTestSupport.join(SharedStoreTest.second.getAll(ids)).size());
	}

	@Test
	public void savedObjectsCanBeRead() throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setBalance(100L);
		account.getTags().add("founder");
		HestiaTestSupport.join(SharedStoreTest.first.save(account));

		final TestAccount read = HestiaTestSupport.join(SharedStoreTest.second.get(account.getId()));
		assertEquals(100L, read.getBalance());
		assertEquals(1L, read.getVersion());
		assertEquals(Arrays.asList("founder"), read.getTags());
	}

	@Test
	public void missingObjectsAreNull() throws Exception {
		assertNull(HestiaTestSupport.join(SharedStoreTest.first.get(HestiaTestSupport.randomId())));
	}

	@Test
	public void deletedObjectsDisappear() throws Exception {
		final TestAccount account = SharedStoreTest.account(1L);
		HestiaTestSupport.join(SharedStoreTest.first.delete(account));
		assertNull(HestiaTestSupport.join(SharedStoreTest.second.get(account.getId())));
	}

	@Test
	public void listenersCanCancelSaves() throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		final SharedStoreListener<TestAccount> listener = new SharedStoreListener<TestAccount>() {

			@Override
			public boolean onPreSave(final TestAccount object, final boolean create) {
				return object.getId().equals(account.getId());
			}

		};

		SharedStoreTest.first.listen(listener);
		HestiaTestSupport.join(SharedStoreTest.first.save(account));
		assertNull(HestiaTestSupport.join(SharedStoreTest.first.get(account.getId())));
	}

	@Test
	public void lostLocksRejectTheWrite() throws Exception {
		final TestAccount account = SharedStoreTest.account(10L);
		account.setBalance(20L);

		final ExecutionException error = assertThrows(ExecutionException.class, () -> HestiaTestSupport.join(SharedStoreTest.first.save(account, new RedisLockToken("lock:" + account.getId(), "expired"))));
		assertInstanceOf(RedisLockLostException.class, error.getCause());
		assertEquals(10L, HestiaTestSupport.join(SharedStoreTest.second.get(account.getId())).getBalance());
	}

	@Test
	public void versionsFollowEverySave() throws Exception {
		final TestAccount account = SharedStoreTest.account(0L);
		account.setBalance(1L);
		HestiaTestSupport.join(SharedStoreTest.first.save(account));

		final Map<String, Long> versions = HestiaTestSupport.join(SharedStoreTest.second.getVersions());
		assertEquals(2L, versions.get(account.getId()));
		assertEquals(2L, HestiaTestSupport.join(SharedStoreTest.second.getVersion(account.getId())));
	}

	@Test
	public void indexedFieldsCanBeSearched() throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setOwner("owner" + account.getId());
		HestiaTestSupport.join(SharedStoreTest.first.save(account));

		assertTrue(HestiaTestSupport.await(() -> SharedStoreTest.first.findOne("@owner:{" + account.getOwner() + "}").join() != null));
		assertEquals(account.getId(), HestiaTestSupport.join(SharedStoreTest.first.findOne("@owner:{" + account.getOwner() + "}")).getId());
	}

	@Test
	public void heldLocksAllowGuardedWrites() throws Exception {
		final TestAccount account = SharedStoreTest.account(10L);
		HestiaTestSupport.join(SharedStoreTest.firstClient.getLock().withLock("lock:" + account.getId(), token -> {
			account.setBalance(30L);
			return SharedStoreTest.first.save(account, token);
		}));
		assertEquals(30L, HestiaTestSupport.join(SharedStoreTest.second.get(account.getId())).getBalance());
	}

	@Test
	public void foreignKeysAreIgnoredByTheScan() throws Exception {
		final String foreign = "testaccount:lock:" + HestiaTestSupport.randomId();
		try (Jedis admin = HestiaTestSupport.admin()) {
			admin.set(foreign, "1");
		}
		assertFalse(HestiaTestSupport.join(SharedStoreTest.first.getVersions()).containsKey(foreign.substring("testaccount:".length())));
	}

	@Test
	public void concurrentReadsGetTheirOwnInstance() throws Exception {
		final TestAccount account = SharedStoreTest.account(5L);
		final CompletableFuture<TestAccount> firstRead = SharedStoreTest.first.get(account.getId());
		final CompletableFuture<TestAccount> secondRead = SharedStoreTest.first.get(account.getId());
		assertNotSame(HestiaTestSupport.join(firstRead), HestiaTestSupport.join(secondRead));
	}

	@Test
	public void concurrentServersMergeTheirChanges() throws Exception {
		final TestAccount account = SharedStoreTest.account(1000L);
		final TestAccount onFirst = HestiaTestSupport.join(SharedStoreTest.first.get(account.getId()));
		final TestAccount onSecond = HestiaTestSupport.join(SharedStoreTest.second.get(account.getId()));
		onFirst.setBalance(onFirst.getBalance() + 100L);
		onFirst.getTags().add("first");
		onFirst.setLastSeen(111L);
		onSecond.setBalance(onSecond.getBalance() - 30L);
		onSecond.getTags().add("second");
		onSecond.setLastSeen(222L);

		HestiaTestSupport.join(SharedStoreTest.first.save(onFirst));
		HestiaTestSupport.join(SharedStoreTest.second.save(onSecond));

		final TestAccount merged = HestiaTestSupport.join(SharedStoreTest.first.get(account.getId()));
		assertEquals(1070L, merged.getBalance());
		assertEquals(222L, merged.getLastSeen());
		assertEquals(3L, merged.getVersion());
		assertEquals(new HashSet<>(Arrays.asList("first", "second")), new HashSet<>(merged.getTags()));
	}

	@Test
	public void parallelWritersNeverLoseAnIncrement() throws Exception {
		final TestAccount account = SharedStoreTest.account(0L);
		final ExecutorService pool = Executors.newFixedThreadPool(8);
		final List<Future<?>> tasks = new ArrayList<>();
		for (int thread = 0; thread < 8; thread++) {
			final SharedStore<TestAccount> store = thread % 2 == 0 ? SharedStoreTest.first : SharedStoreTest.second;
			tasks.add(pool.submit(() -> {
				for (int i = 0; i < 25; i++) {
					final TestAccount copy = HestiaTestSupport.join(store.get(account.getId()));
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

		final TestAccount stored = HestiaTestSupport.join(SharedStoreTest.first.get(account.getId()));
		assertNotNull(stored);
		assertEquals(200L, stored.getBalance());
		assertEquals(201L, stored.getVersion());
	}

	private static TestAccount account(final long balance) throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setBalance(balance);
		HestiaTestSupport.join(SharedStoreTest.first.save(account));
		return account;
	}

}