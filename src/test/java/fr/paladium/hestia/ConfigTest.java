package fr.paladium.hestia;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import fr.paladium.hestia.cache.MemoryCacheTransport;
import fr.paladium.hestia.cache.RedisCacheConfig;
import fr.paladium.hestia.model.TestAccount;
import fr.paladium.hestia.redis.RedisConfig;
import fr.paladium.hestia.redis.json.RedisJsonTypeResolver;
import fr.paladium.hestia.redis.metric.RedisMetricConfig;
import fr.paladium.hestia.store.RedisStoreConfig;

public class ConfigTest {

	@Test
	public void cacheDefaults() {
		final RedisCacheConfig config = RedisCacheConfig.create();
		assertNull(config.getTransport());
		assertEquals(Duration.ofMinutes(1L), config.getRefreshInterval());

		final MemoryCacheTransport transport = new MemoryCacheTransport(new ArrayList<>());
		assertSame(transport, config.<RedisCacheConfig>transport(transport).getTransport());
	}

	@Test
	public void redisDefaults() {
		final RedisConfig config = RedisConfig.create("host");
		assertEquals(6379, config.getPort());
		assertEquals(0, config.getDatabase());
		assertEquals(16, config.getPoolSize());
		assertNull(config.getPassword());
		assertNull(config.getMetrics());
		assertSame(RedisJsonTypeResolver.IDENTITY, config.getTypeResolver());
	}

	@Test
	public void storeDefaults() {
		final RedisStoreConfig<TestAccount> config = RedisStoreConfig.create(TestAccount.class, TestAccount::getId);
		assertEquals("testaccount", config.getName());
		assertEquals(128, config.getThreads());
		assertEquals(100, config.getBatchSize());
		assertEquals(5, config.getPatchAttempts());
		assertTrue(config.getKeyFilter().test("abc"));
		assertFalse(config.getKeyFilter().test("lock:abc"));
	}

	@Test
	public void metricDefaults() {
		final RedisMetricConfig config = RedisMetricConfig.create("prefix");
		assertEquals("prefix", config.getPrefix());
		assertEquals(Duration.ofDays(7L), config.getRetention());
		assertEquals(Duration.ofSeconds(10L), config.getFlushInterval());
	}

	@Test
	public void fluentSettersReturnTheSameConfig() {
		final RedisConfig config = RedisConfig.create("host");
		assertSame(config, config.<RedisConfig>port(1).<RedisConfig>database(2).<RedisConfig>poolSize(3).password("secret"));
		assertEquals(1, config.getPort());
		assertEquals(2, config.getDatabase());
		assertEquals(3, config.getPoolSize());
		assertEquals("secret", config.getPassword());
	}

}