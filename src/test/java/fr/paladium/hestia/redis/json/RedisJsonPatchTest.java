package fr.paladium.hestia.redis.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonPrimitive;

import fr.paladium.hestia.redis.impl.RedisProtocol;
import fr.paladium.hestia.redis.lock.RedisLockToken;

public class RedisJsonPatchTest {

	private static final Gson GSON = new Gson();

	@Test
	public void markersUseTheKeyNamespace() {
		final RedisJsonPatch patch = RedisJsonPatch.create("faction:abc", RedisJsonPatchTest.GSON);
		assertEquals("faction-patch:" + patch.getId(), patch.getMarker());
		assertTrue(RedisJsonPatch.create("plain", RedisJsonPatchTest.GSON).getMarker().startsWith("plain-patch:"));
	}

	@Test
	public void operationsAreEncodedByFive() {
		final RedisJsonPatch patch = RedisJsonPatch.create("faction:abc", RedisJsonPatchTest.GSON).set("$.a", "$", new JsonPrimitive(1)).remove("$.b", new JsonPrimitive("x"));
		assertEquals(2, patch.size());
		assertEquals(10, patch.getOperations().size());
	}

	@Test
	public void unguardedPatchesUseTwoKeys() {
		final RedisJsonPatch patch = RedisJsonPatch.create("faction:abc", RedisJsonPatchTest.GSON).increment("$.balance", "$", "5", new JsonPrimitive(15));
		final List<String> args = Arrays.asList(patch.toCommand().getArgs());
		assertEquals(RedisProtocol.EVAL, patch.toCommand().getCommand());
		assertEquals(Arrays.asList("2", "faction:abc", patch.getMarker(), "600", "", "1", "I", "$.balance", "$", "5", "15"), args.subList(1, args.size()));
	}

	@Test
	public void guardedPatchesSendTheLockAsThirdKey() {
		final RedisJsonPatch patch = RedisJsonPatch.create("faction:abc", RedisJsonPatchTest.GSON).delete("$.name").guard(new RedisLockToken("lock:abc", "token"));
		final List<String> args = Arrays.asList(patch.toCommand().getArgs());
		assertEquals(Arrays.asList("3", "faction:abc", patch.getMarker(), "lock:abc", "600", "token", "1", "D", "$.name", "", "", ""), args.subList(1, args.size()));
	}

}