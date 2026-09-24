package fr.paladium.hestia.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import fr.paladium.hestia.HestiaTestSupport;
import fr.paladium.hestia.cache.transport.CacheMessage;
import fr.paladium.hestia.model.TestAccount;
import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.store.RedisStore;
import fr.paladium.hestia.store.RedisStoreConfig;

@Tag("integration")
public class RedisCacheTest {

	private static final Gson GSON = new Gson();
	private static final List<String> UPDATES = new CopyOnWriteArrayList<>();

	private static RedisClient firstClient;
	private static RedisClient secondClient;
	private static RedisCache<TestAccount> first;
	private static RedisCache<TestAccount> second;

	@AfterAll
	public static void disconnect() {
		RedisCacheTest.first.close();
		RedisCacheTest.second.close();
		RedisCacheTest.first.getStore().close();
		RedisCacheTest.second.getStore().close();
		RedisCacheTest.firstClient.close();
		RedisCacheTest.secondClient.close();
	}

	@BeforeAll
	public static void connect() throws Exception {
		RedisCacheTest.firstClient = HestiaTestSupport.client();
		RedisCacheTest.secondClient = HestiaTestSupport.client();
		RedisCacheTest.first = RedisCacheTest.cache(RedisCacheTest.firstClient, RedisCacheConfig.create());
		RedisCacheTest.second = RedisCacheTest.cache(RedisCacheTest.secondClient, RedisCacheConfig.create());
		RedisCacheTest.second.listen(new RedisCacheListener<TestAccount>() {

			@Override
			public void onPostUpdate(final TestAccount previous, final TestAccount current) {
				RedisCacheTest.UPDATES.add(current.getId() + ":" + current.getVersion());
			}

		});
		HestiaTestSupport.join(RedisCacheTest.first.start());
		HestiaTestSupport.join(RedisCacheTest.second.start());
	}

	@Test
	public void cachesRequireAVersionField() {
		final RedisStore<String> store = RedisStore.create(RedisCacheTest.firstClient, RedisStoreConfig.create(String.class, value -> value));
		assertThrows(IllegalArgumentException.class, () -> RedisCache.create(store, RedisCacheConfig.create()));
		store.close();
	}

	@Test
	public void savesReachTheOtherServers() throws Exception {
		final TestAccount account = RedisCacheTest.account(42L);
		assertTrue(HestiaTestSupport.await(() -> RedisCacheTest.second.get(account.getId()).map(cached -> cached.getBalance() == 42L).orElse(false)));
		assertTrue(RedisCacheTest.first.get(account.getId()).isPresent());
		assertTrue(RedisCacheTest.UPDATES.contains(account.getId() + ":1"));
	}

	@Test
	public void deletesReachTheOtherServers() throws Exception {
		final TestAccount account = RedisCacheTest.account(1L);
		assertTrue(HestiaTestSupport.await(() -> RedisCacheTest.second.get(account.getId()).isPresent()));

		HestiaTestSupport.join(RedisCacheTest.first.getStore().delete(account));
		assertFalse(RedisCacheTest.first.get(account.getId()).isPresent());
		assertTrue(HestiaTestSupport.await(() -> !RedisCacheTest.second.get(account.getId()).isPresent()));
	}

	@Test
	public void refreshRepairsMissedMessages() throws Exception {
		final TestAccount account = RedisCacheTest.account(5L);
		assertTrue(HestiaTestSupport.await(() -> RedisCacheTest.second.get(account.getId()).isPresent()));

		account.setBalance(9L);
		account.setVersion(7L);
		RedisCacheTest.firstClient.execute(RedisCommand.Json.set(RedisCacheTest.GSON, "testaccount:" + account.getId(), account));
		HestiaTestSupport.join(RedisCacheTest.second.refresh());
		assertEquals(9L, RedisCacheTest.second.get(account.getId()).get().getBalance());
	}

	@Test
	public void invalidateReloadsStaleEntries() throws Exception {
		final TestAccount account = RedisCacheTest.account(5L);
		assertTrue(HestiaTestSupport.await(() -> RedisCacheTest.second.get(account.getId()).isPresent()));

		account.setBalance(8L);
		account.setVersion(4L);
		RedisCacheTest.firstClient.execute(RedisCommand.Json.set(RedisCacheTest.GSON, "testaccount:" + account.getId(), account));
		HestiaTestSupport.join(RedisCacheTest.second.invalidate(account.getId()));
		assertEquals(8L, RedisCacheTest.second.get(account.getId()).get().getBalance());
	}

	@Test
	public void olderVersionsNeverReplaceNewerOnes() throws Exception {
		final TestAccount account = RedisCacheTest.account(1L);
		account.setBalance(2L);
		HestiaTestSupport.join(RedisCacheTest.first.getStore().save(account));
		assertTrue(HestiaTestSupport.await(() -> RedisCacheTest.second.get(account.getId()).map(cached -> cached.getVersion() == 2L).orElse(false)));

		HestiaTestSupport.join(RedisCacheTest.second.invalidate(account.getId()));
		assertEquals(2L, RedisCacheTest.second.get(account.getId()).get().getBalance());
	}

	@Test
	public void customTransportsCarryTheSynchronization() throws Exception {
		final List<Consumer<CacheMessage>> bus = new CopyOnWriteArrayList<>();
		final MemoryCacheTransport transport = new MemoryCacheTransport(bus);
		final RedisCache<TestAccount> sender = RedisCacheTest.cache(RedisCacheTest.firstClient, RedisCacheConfig.create().transport(transport));
		final RedisCache<TestAccount> receiver = RedisCacheTest.cache(RedisCacheTest.secondClient, RedisCacheConfig.create().transport(new MemoryCacheTransport(bus)));
		HestiaTestSupport.join(sender.start());
		HestiaTestSupport.join(receiver.start());

		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setBalance(7L);
		HestiaTestSupport.join(sender.getStore().save(account));
		final boolean synced = receiver.get(account.getId()).map(cached -> cached.getBalance() == 7L).orElse(false);

		HestiaTestSupport.join(sender.getStore().delete(account));
		final boolean removed = !receiver.get(account.getId()).isPresent();
		final CacheMessage last = transport.getPublished().get(transport.getPublished().size() - 1);

		sender.close();
		receiver.close();
		sender.getStore().close();
		receiver.getStore().close();
		assertTrue(synced);
		assertTrue(removed);
		assertEquals(account.getId(), last.getId());
		assertNull(last.getJson());
	}

	private static TestAccount account(final long balance) throws Exception {
		final TestAccount account = new TestAccount(HestiaTestSupport.randomId());
		account.setBalance(balance);
		HestiaTestSupport.join(RedisCacheTest.first.getStore().save(account));
		return account;
	}

	private static RedisCache<TestAccount> cache(final RedisClient client, final RedisCacheConfig config) {
		final RedisStore<TestAccount> store = RedisStore.create(client, RedisStoreConfig.create(TestAccount.class, TestAccount::getId));
		return RedisCache.create(store, config);
	}

}