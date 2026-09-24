package fr.paladium.hestia;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.RedisConfig;
import fr.paladium.hestia.redis.exception.RedisConnectionException;
import redis.clients.jedis.Jedis;

public final class HestiaTestSupport {

	private static final String IMAGE = "redis:8.6.2";
	private static final GenericContainer<?> REDIS = HestiaTestSupport.start();

	private HestiaTestSupport() {}

	public static Jedis admin() {
		return new Jedis(HestiaTestSupport.REDIS.getHost(), HestiaTestSupport.REDIS.getMappedPort(6379));
	}

	public static String randomId() {
		return UUID.randomUUID().toString().replace("-", "");
	}

	public static RedisConfig config() {
		return RedisConfig.create(HestiaTestSupport.REDIS.getHost()).port(HestiaTestSupport.REDIS.getMappedPort(6379));
	}

	public static RedisClient client() throws RedisConnectionException {
		return RedisClient.create(HestiaTestSupport.config());
	}

	public static <T> T join(final CompletableFuture<T> future) throws Exception {
		return future.get(15L, TimeUnit.SECONDS);
	}

	public static boolean await(final BooleanSupplier condition) throws InterruptedException {
		final long deadline = System.currentTimeMillis() + 10000L;
		while (System.currentTimeMillis() < deadline) {
			if (condition.getAsBoolean()) {
				return true;
			}
			Thread.sleep(25L);
		}
		return condition.getAsBoolean();
	}

	private static GenericContainer<?> start() {
		final GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(HestiaTestSupport.IMAGE)).withExposedPorts(6379);
		container.start();
		return container;
	}

}