package fr.paladium.hestia.redis.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

import fr.paladium.hestia.model.TestAccount;

public class RedisJsonDiffResolverTest {

	private static final Gson GSON = new Gson();

	@Test
	public void addedKeysAreSet() {
		assertEquals(Arrays.asList("S", "$.owner", "$", "\"alice\"", ""), RedisJsonDiffResolverTest.diff("{}", "{\"owner\":\"alice\"}").getOperations());
	}

	@Test
	public void typeChangesAreSet() {
		assertTrue(RedisJsonDiffResolverTest.diff("{\"owner\":\"alice\"}", "{\"owner\":{\"name\":\"alice\"}}").getOperations().contains("S"));
	}

	@Test
	public void mapValuesStayDeltas() {
		assertEquals(Arrays.asList("I", "$.counters.kills", "$.counters", "2", "3"), RedisJsonDiffResolverTest.diff("{\"counters\":{\"kills\":1}}", "{\"counters\":{\"kills\":3}}").getOperations());
	}

	@Test
	public void specialKeysAreEscaped() {
		assertEquals("$.counters[\"a-b\"]", RedisJsonDiffResolverTest.diff("{\"counters\":{\"a-b\":1}}", "{\"counters\":{\"a-b\":2}}").getOperations().get(1));
	}

	@Test
	public void removedKeysAreDeleted() {
		assertEquals(Arrays.asList("D", "$.owner", "", "", ""), RedisJsonDiffResolverTest.diff("{\"owner\":\"alice\"}", "{}").getOperations());
	}

	@Test
	public void overwrittenFieldsAreSet() {
		assertEquals(Arrays.asList("S", "$.lastSeen", "$", "5", ""), RedisJsonDiffResolverTest.diff("{\"lastSeen\":1}", "{\"lastSeen\":5}").getOperations());
	}

	@Test
	public void numbersAreSentAsExactDeltas() {
		assertEquals(Arrays.asList("I", "$.balance", "$", "0.3", "10.4"), RedisJsonDiffResolverTest.diff("{\"balance\":10.1}", "{\"balance\":10.4}").getOperations());
	}

	@Test
	public void appendedElementsAreAppended() {
		assertEquals(Arrays.asList("A", "$.tags", "$", "\"b\"", "[\"a\",\"b\"]"), RedisJsonDiffResolverTest.diff("{\"tags\":[\"a\"]}", "{\"tags\":[\"a\",\"b\"]}").getOperations());
	}

	@Test
	public void duplicatedArraysAreReplaced() {
		assertEquals(Collections.singletonList("S"), RedisJsonDiffResolverTest.diff("{\"tags\":[\"a\",\"a\"]}", "{\"tags\":[\"b\"]}").getOperations().subList(0, 1));
	}

	@Test
	public void primitiveSetsAreMergedByValue() {
		assertEquals(Arrays.asList("R", "$.tags", "", "\"a\"", "", "A", "$.tags", "$", "\"c\"", "[\"b\",\"c\"]"), RedisJsonDiffResolverTest.diff("{\"tags\":[\"a\",\"b\"]}", "{\"tags\":[\"b\",\"c\"]}").getOperations());
	}

	@Test
	public void overwriteIsInheritedByChildren() {
		assertEquals(Arrays.asList("S", "$.baselines.kills", "$.baselines", "3", ""), RedisJsonDiffResolverTest.diff("{\"baselines\":{\"kills\":1}}", "{\"baselines\":{\"kills\":3}}").getOperations());
	}

	@Test
	public void identicalDocumentsProduceNoOperation() {
		assertEquals(0, RedisJsonDiffResolverTest.diff("{\"balance\":1,\"tags\":[\"a\"]}", "{\"balance\":1,\"tags\":[\"a\"]}").size());
	}

	private static RedisJsonPatch diff(final String snapshot, final String current) {
		final RedisJsonPatch patch = RedisJsonPatch.create("testaccount:1", RedisJsonDiffResolverTest.GSON);
		RedisJsonDiffResolver.resolve(patch, RedisJsonSchema.of(TestAccount.class, RedisJsonTypeResolver.IDENTITY), JsonParser.parseString(snapshot), JsonParser.parseString(current));
		return patch;
	}

}