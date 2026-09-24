package fr.paladium.hestia.redis.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import fr.paladium.hestia.HestiaTestSupport;
import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.lock.RedisLockToken;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

@Tag("integration")
public class RedisJsonPatchScriptTest {

	private static final Gson GSON = new Gson();

	private static RedisClient client;

	@AfterAll
	public static void disconnect() {
		RedisJsonPatchScriptTest.client.close();
	}

	@BeforeAll
	public static void connect() throws Exception {
		RedisJsonPatchScriptTest.client = HestiaTestSupport.client();
	}

	@Test
	public void replayIsAppliedOnce() {
		final String key = RedisJsonPatchScriptTest.document("{\"balance\":10}");
		final RedisCommand command = RedisJsonPatch.create(key, RedisJsonPatchScriptTest.GSON).increment("$.balance", "$", "5", new JsonPrimitive(15)).toCommand();
		final List<Object> first = RedisJsonPatchScriptTest.client.execute(command);
		final List<Object> second = RedisJsonPatchScriptTest.client.execute(command);

		assertEquals(RedisJsonPatch.APPLIED, first.get(0));
		assertEquals(RedisJsonPatch.REPLAYED, second.get(0));
		assertEquals(15L, RedisJsonPatchScriptTest.read(key).get("balance").getAsLong());
	}

	@Test
	public void guardRejectsLostLocks() {
		final String key = RedisJsonPatchScriptTest.document("{\"balance\":10}");
		final String lock = "lock:" + HestiaTestSupport.randomId();
		try (Jedis admin = HestiaTestSupport.admin()) {
			admin.set(lock, "another-owner");
		}

		final RedisJsonPatch patch = RedisJsonPatch.create(key, RedisJsonPatchScriptTest.GSON).increment("$.balance", "$", "5", new JsonPrimitive(15)).guard(new RedisLockToken(lock, "mine"));
		final List<Object> reply = RedisJsonPatchScriptTest.client.execute(patch.toCommand());

		assertEquals(RedisJsonPatch.REJECTED, reply.get(0));
		assertEquals(10L, RedisJsonPatchScriptTest.read(key).get("balance").getAsLong());
	}

	@Test
	public void outOfMemoryWritesNothing() {
		final String key = RedisJsonPatchScriptTest.document("{\"balance\":10}");
		final RedisJsonPatch patch = RedisJsonPatch.create(key, RedisJsonPatchScriptTest.GSON).increment("$.balance", "$", "5", new JsonPrimitive(15));
		final JedisDataException error;
		try (Jedis admin = HestiaTestSupport.admin()) {
			admin.configSet("maxmemory", "1");
			try {
				error = assertThrows(JedisDataException.class, () -> RedisJsonPatchScriptTest.client.execute(patch.toCommand()));
			} finally {
				admin.configSet("maxmemory", "0");
			}
		}

		assertTrue(RedisClient.isTransient(error));
		assertEquals(10L, RedisJsonPatchScriptTest.read(key).get("balance").getAsLong());
		assertFalse(RedisJsonPatchScriptTest.client.<Boolean>execute(RedisCommand.Base.exists(patch.getMarker())));
	}

	@Test
	public void operationsAreAppliedTogether() {
		final String key = RedisJsonPatchScriptTest.document("{\"balance\":10,\"tags\":[\"a\",\"b\"],\"owner\":\"alice\"}");
		final RedisJsonPatch patch = RedisJsonPatch.create(key, RedisJsonPatchScriptTest.GSON)
			.increment("$.balance", "$", "-4", new JsonPrimitive(6))
			.remove("$.tags", new JsonPrimitive("a"))
			.append("$.tags", "$", new JsonPrimitive("c"), JsonParser.parseString("[\"b\",\"c\"]"))
			.delete("$.owner");
		final List<Object> reply = RedisJsonPatchScriptTest.client.execute(patch.toCommand());

		final JsonObject stored = RedisJsonPatchScriptTest.read(key);
		assertEquals(RedisJsonPatch.APPLIED, reply.get(0));
		assertEquals(6L, stored.get("balance").getAsLong());
		assertEquals(JsonParser.parseString("[\"b\",\"c\"]"), stored.get("tags"));
		assertFalse(stored.has("owner"));
	}

	@Test
	public void vanishedPathsFallBackToTheTargetValue() {
		final String key = RedisJsonPatchScriptTest.document("{\"counters\":{}}");
		final RedisJsonPatch patch = RedisJsonPatch.create(key, RedisJsonPatchScriptTest.GSON)
			.increment("$.counters.kills", "$.counters", "2", new JsonPrimitive(3))
			.append("$.counters.tags", "$.counters", new JsonPrimitive("a"), JsonParser.parseString("[\"a\"]"))
			.set("$.missing.value", "$.missing", new JsonPrimitive(1));
		final List<Object> reply = RedisJsonPatchScriptTest.client.execute(patch.toCommand());

		final JsonObject stored = RedisJsonPatchScriptTest.read(key);
		assertEquals(RedisJsonPatch.APPLIED, reply.get(0));
		assertEquals(3L, stored.getAsJsonObject("counters").get("kills").getAsLong());
		assertEquals(JsonParser.parseString("[\"a\"]"), stored.getAsJsonObject("counters").get("tags"));
		assertFalse(stored.has("missing"));
	}

	private static JsonObject read(final String key) {
		return JsonParser.parseString(RedisJsonPatchScriptTest.client.<String>execute(RedisCommand.Json.get(key))).getAsJsonObject();
	}

	private static String document(final String json) {
		final String key = "patch:" + HestiaTestSupport.randomId();
		RedisJsonPatchScriptTest.client.execute(RedisCommand.Json.set(RedisJsonPatchScriptTest.GSON, key, JsonParser.parseString(json)));
		return key;
	}

}