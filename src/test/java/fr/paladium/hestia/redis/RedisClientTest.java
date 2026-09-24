package fr.paladium.hestia.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import fr.paladium.hestia.HestiaTestSupport;
import fr.paladium.hestia.redis.exception.RedisConnectionException;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.pubsub.RedisSubscription;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

@Tag("integration")
public class RedisClientTest {

	private static RedisClient client;

	@AfterAll
	public static void disconnect() {
		RedisClientTest.client.close();
	}

	@BeforeAll
	public static void connect() throws Exception {
		RedisClientTest.client = HestiaTestSupport.client();
	}

	@Test
	public void valuesAreDecodedAsUtf8() {
		final String key = "utf8:" + HestiaTestSupport.randomId();
		RedisClientTest.client.execute(RedisCommand.Base.set(key, "Fâché à l'été ✓"));
		assertEquals("Fâché à l'été ✓", RedisClientTest.client.<String>execute(RedisCommand.Base.get(key)));
	}

	@Test
	public void unreachableServerFailsFast() {
		assertThrows(RedisConnectionException.class, () -> RedisClient.create(RedisConfig.create("127.0.0.1").port(1)));
	}

	@Test
	public void batchesFailOnTheFirstError() {
		final String key = "batch:" + HestiaTestSupport.randomId();
		RedisClientTest.client.execute(RedisCommand.Base.set(key, "text"));
		assertThrows(JedisDataException.class, () -> RedisClientTest.client.execute(RedisCommand.Base.get(key), RedisCommand.Base.hgetall(key)));
	}

	@Test
	public void scanReturnsEveryMatchingKey() {
		final String prefix = "scan:" + HestiaTestSupport.randomId() + ":";
		for (int i = 0; i < 1500; i++) {
			RedisClientTest.client.execute(RedisCommand.Base.set(prefix + i, "1"));
		}
		assertEquals(1500, RedisClientTest.client.scan(prefix + "*").size());
	}

	@Test
	public void transientErrorsAreRecognized() {
		assertTrue(RedisClient.isTransient(new JedisConnectionException("reset")));
		assertTrue(RedisClient.isTransient(new JedisDataException("LOADING Redis is loading the dataset in memory")));
		assertFalse(RedisClient.isTransient(new JedisDataException("ERR wrong number of arguments")));
		assertFalse(RedisClient.isTransient(new IllegalStateException("OOM")));
	}

	@Test
	public void subscriptionsReceiveMessages() throws Exception {
		final String channel = "channel:" + HestiaTestSupport.randomId();
		final List<String> received = new CopyOnWriteArrayList<>();
		try (RedisSubscription subscription = RedisClientTest.client.subscribe(channel, received::add)) {
			assertTrue(HestiaTestSupport.await(() -> {
				RedisClientTest.client.execute(RedisCommand.Base.publish(channel, "hello"));
				return !received.isEmpty();
			}));
		}
		assertEquals("hello", received.get(0));
	}

}