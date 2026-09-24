package fr.paladium.hestia.redis.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import fr.paladium.hestia.model.TestAccount;
import fr.paladium.hestia.model.TestInvalidVersion;

public class RedisJsonSerializerTest {

	private static final String KEY = "testaccount:serializer";

	@Test
	public void versionsMustBeLongFields() {
		assertThrows(IllegalArgumentException.class, () -> RedisJsonSerializerTest.serializer().getVersion(new TestInvalidVersion()));
	}

	@Test
	public void onlyObjectsCanBeCaptured() {
		assertThrows(IllegalArgumentException.class, () -> RedisJsonSerializerTest.serializer().capture("text"));
	}

	@Test
	public void bindAttachesTheLoadedState() {
		final TestAccount account = new TestAccount("bind");
		final JsonElement tree = JsonParser.parseString("{\"version\":3}");
		RedisJsonSerializerTest.serializer().bind(account, tree);
		assertSame(tree, account.getSnapshot());
	}

	@Test
	public void firstSaveWritesTheWholeDocument() {
		final RedisJsonSerializer serializer = RedisJsonSerializerTest.serializer();
		final TestAccount account = new TestAccount("first");
		final RedisJsonSerializeResult result = serializer.prepare(RedisJsonSerializerTest.KEY, account, serializer.capture(account));
		final List<String> operations = result.getPatch().getOperations();

		assertEquals(Arrays.asList("S", "$", ""), operations.subList(0, 3));
		assertEquals(1L, JsonParser.parseString(operations.get(3)).getAsJsonObject().get("version").getAsLong());
		assertEquals(0L, account.getVersion());
	}

	@Test
	public void olderSnapshotsNeverReplaceNewerOnes() {
		final RedisJsonSerializer serializer = RedisJsonSerializerTest.serializer();
		serializer.snapshot(RedisJsonSerializerTest.KEY, "{\"id\":\"old\",\"version\":5}", TestAccount.class);
		serializer.snapshot(RedisJsonSerializerTest.KEY, "{\"id\":\"old\",\"version\":3}", TestAccount.class);

		final TestAccount account = new TestAccount("old");
		final RedisJsonSerializeResult result = serializer.prepare(RedisJsonSerializerTest.KEY, account, serializer.capture(account));
		assertTrue(RedisJsonSerializerTest.contains(result.getPatch().getOperations(), "I", "$.version", "$", "1", "6"));
	}

	@Test
	public void theObjectSnapshotWinsOverTheKeySnapshot() {
		final RedisJsonSerializer serializer = RedisJsonSerializerTest.serializer();
		serializer.snapshot(RedisJsonSerializerTest.KEY, "{\"id\":\"mine\",\"version\":10}", TestAccount.class);

		final TestAccount account = new TestAccount("mine");
		account.setVersion(2L);
		serializer.bind(account, serializer.capture(account));
		account.setBalance(7L);

		final RedisJsonSerializeResult result = serializer.prepare(RedisJsonSerializerTest.KEY, account, serializer.capture(account));
		assertTrue(RedisJsonSerializerTest.contains(result.getPatch().getOperations(), "I", "$.balance", "$", "7", "7"));
		assertTrue(RedisJsonSerializerTest.contains(result.getPatch().getOperations(), "I", "$.version", "$", "1", "3"));
	}

	@Test
	public void commitUpdatesTheObjectAndTheKeySnapshot() {
		final RedisJsonSerializer serializer = RedisJsonSerializerTest.serializer();
		final TestAccount account = new TestAccount("commit");
		final RedisJsonSerializeResult result = serializer.prepare(RedisJsonSerializerTest.KEY, account, serializer.capture(account));
		result.getCommitLocal().run();

		assertEquals(1L, account.getVersion());
		assertEquals(1L, serializer.getVersion(account));
		assertEquals(1L, account.getSnapshot().getAsJsonObject().get("version").getAsLong());
		assertTrue(serializer.hasSnapshot(RedisJsonSerializerTest.KEY));

		serializer.removeSnapshot(RedisJsonSerializerTest.KEY);
		assertFalse(serializer.hasSnapshot(RedisJsonSerializerTest.KEY));
	}

	private static RedisJsonSerializer serializer() {
		return RedisJsonSerializer.create(new Gson(), RedisJsonTypeResolver.IDENTITY);
	}

	private static boolean contains(final List<String> operations, final String... operation) {
		for (int i = 0; i + 5 <= operations.size(); i += 5) {
			if (operations.subList(i, i + 5).equals(Arrays.asList(operation))) {
				return true;
			}
		}
		return false;
	}

}