package fr.paladium.hestia.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import fr.paladium.hestia.HestiaTestSupport;
import fr.paladium.hestia.model.TestAccount;
import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.store.SharedStore;
import fr.paladium.hestia.store.SharedStoreConfig;

public class SharedCacheTest {

	private static final Gson GSON = new Gson();
	private static final List<String> UPDATES = new CopyOnWriteArrayList<>();

	private static RedisClient firstClient;
	private static RedisClient secondClient;
	private static SharedCache<TestAccount> first;
	private static SharedCache<TestAccount> second;

	@AfterAll
	public static void disconnect() {
		SharedCacheTest.first.close();
		SharedCacheTest.second.close();
		SharedCacheTest.first.getStore().close();
		SharedCacheTest.second.getStore().close();
		SharedCacheTest.firstClient.close();
		SharedCacheTest.secondClient.close();
	}

	@BeforeAll
	public static void connect() throws Exception {
		SharedCacheTest.firstClient = HestiaTestSupport.client();
		SharedCacheTest.secondClient = HestiaTestSupport.client();
		SharedCacheTest.first = SharedCacheTest.cache(SharedCacheTest.firstClient);
		SharedCacheTest.second = SharedCacheTest.cache(SharedCacheTest.secondClient);
		SharedCacheTest.second.listen(new SharedCacheListener<TestAccount>() {

			@Override
			public void onUpdate(final TestAccount previous, final TestAccount current) {
				SharedCacheTest.UPDATES.add(current.getId() + ":" + current.getVersion());
			}

		});
		HestiaTestSupport.join(SharedCacheTest.first.start());
		HestiaTestSupport.join(SharedCacheTest.second.start());
	}

	@Test
	public void cachesRequireAVersionField() {
		final SharedStore<String> store = SharedStore.create(SharedCacheTest.firstClient, SharedStoreConfig.create(String.class, value -> value));
		assertThrows(IllegalArgumentException.class, () -> SharedCache.create(store, SharedCacheConfig.create()));
		store.close();
	}

	@Test
	public void savesReachTheOtherServers() throws Exception {
		final TestAccount account = SharedCacheTest.account(42L);
		assertTrue(HestiaTestSupport.await(() -> SharedCacheTest.second.get(account.getId()).map(cached -> cached.getBalance() == 42L).orElse(false)));
		assertTrue(SharedCacheTest.first.get(account.getId()).isPresent());
		assertTrue(SharedCacheTest.UPDATES.contains(account.getId() + ":1"));
	}

	@Test
	public void deletesReachTheOtherServers() throws Exception {
		final TestAccount account = SharedCacheTest.account(1L);
		assertTrue(HestiaTestSupport.await(() -> SharedCacheTest.second.get(account.getId()).isPresent()));

		HestiaTestSupport.join(SharedCacheTest.first.getStore().delete(account));
		assertFalse(SharedCacheTest.first.get(account.getId()).isPresent());
		assertTrue(HestiaTestSupport.await(() -> !SharedCacheTest.second.get(account.getId()).isPresent()));
	}

	@Test
	public void refreshRepairsMissedMessages() throws Exception {
		final TestAccount account = SharedCacheTest.account(5L);
		assertTrue(HestiaTestSupport.await(() -> SharedCacheTest.second.get(account.getId()).isPresent()));

		account.setBalance(9L);
		account.setVersion(7L);
		SharedCacheTest.firstClient.execute(RedisCommand.Json.set(SharedCacheTest.GSON, "testaccount:" + account.getId(), account));
		HestiaTestSupport.join(SharedCacheTest.second.refresh());
		assertEquals(9L, SharedCacheTest.second.get(account.getId()).get().getBalance());
	}

	@Test
	public void invalidateReloadsStaleEntries() throws Exception {
		final TestAccount account = SharedCacheTest.account(5L);
		assertTrue(HestiaTestSupport.await(() -> SharedCacheTest.second.get(account.getId()).isPresent()));

		account.setBalance(8L);
		account.setVersion(4L);
		SharedCacheTest.firstClient.execute(RedisCommand.Json.set(SharedCacheTest.GSON, "testaccount:" + account.getId(), account));
		HestiaTestSupport.join(SharedCacheTest.second.invalidate(account.getId()));
		assertEquals(8L, SharedCacheTest.second.get(account.getId()).get().getBalance());
	}

	@Test
	public void olderVersionsNeverReplaceNewerOnes() throws Exception {
		final TestAccount account = SharedCacheTest.account(1L);
		account.setBalance(2L);
		HestiaTestSupport.join(SharedCacheTest.first.getStore().save(account));
		assertTrue(HestiaTestSupport.await(() -> SharedCacheTest.second.get(account.getId()).map(cached -> cached.getVersion() == 2L).orElse(false)));

		HestiaTestSupport.join(SharedCacheTest.second.invalidate(account.getId()));
		assertEquals(2L, SharedCacheTest.second.get(account.getId()).get().getBalance());
	}

	private static TestAccount account(final long balance) throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setBalance(balance);
		HestiaTestSupport.join(SharedCacheTest.first.getStore().save(account));
		return account;
	}

	private static SharedCache<TestAccount> cache(final RedisClient client) {
		final SharedStore<TestAccount> store = SharedStore.create(client, SharedStoreConfig.create(TestAccount.class, TestAccount::getId));
		return SharedCache.create(store, SharedCacheConfig.create());
	}

}