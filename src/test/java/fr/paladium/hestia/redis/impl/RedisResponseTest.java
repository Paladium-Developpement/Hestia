package fr.paladium.hestia.redis.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class RedisResponseTest {

	private static final Gson GSON = new Gson();

	@Test
	public void emptySearchesAreEmpty() {
		assertTrue(RedisResponse.searchJsonStringList().parse(Collections.singletonList(0L)).isEmpty());
		assertNull(RedisResponse.searchJsonString().parse(Collections.singletonList(0L)));
	}

	@Test
	public void booleansComeFromCounts() {
		assertTrue(RedisResponse.bool().parse(1L));
		assertFalse(RedisResponse.bool().parse(0L));
		assertFalse(RedisResponse.bool().parse(null));
	}

	@Test
	public void stringsAreDecodedAsUtf8() {
		assertEquals("été ✓", RedisResponse.string().parse(RedisResponseTest.bytes("été ✓")));
		assertNull(RedisResponse.string().parse(null));
	}

	@Test
	public void jsonIsReadWithTheGivenGson() {
		final JsonObject parsed = RedisResponse.json(RedisResponseTest.GSON, JsonObject.class).parse(RedisResponseTest.bytes("{\"a\":1}"));
		assertEquals(1, parsed.get("a").getAsInt());
	}

	@Test
	public void stringListsKeepMissingValues() {
		assertEquals(Arrays.asList("a", null), RedisResponse.stringList().parse(Arrays.asList(RedisResponseTest.bytes("a"), null)));
	}

	@Test
	public void searchResultsExposeTheirDocuments() {
		final List<Object> reply = Arrays.asList(2L, RedisResponseTest.bytes("k1"), Arrays.asList(RedisResponseTest.bytes("$"), RedisResponseTest.bytes("{\"a\":1}")), RedisResponseTest.bytes("k2"), Arrays.asList(RedisResponseTest.bytes("$"), RedisResponseTest.bytes("[{\"a\":2}]")));
		assertEquals(Arrays.asList("{\"a\":1}", "{\"a\":2}"), RedisResponse.searchJsonStringList().parse(reply));
		assertEquals("{\"a\":1}", RedisResponse.searchJsonString().parse(reply));
	}

	@Test
	public void jsonListsDropMissingDocumentsAndBrackets() {
		assertEquals(Arrays.asList("{\"a\":1}", "{\"a\":2}"), RedisResponse.jsonStringList().parse(Arrays.asList(RedisResponseTest.bytes("[{\"a\":1}]"), null, RedisResponseTest.bytes("{\"a\":2}"))));
		assertEquals(2, RedisResponse.jsonList(RedisResponseTest.GSON, JsonObject.class).parse(Arrays.asList(RedisResponseTest.bytes("[{\"a\":1}]"), RedisResponseTest.bytes("{\"a\":2}"))).size());
	}

	private static byte[] bytes(final String value) {
		return value.getBytes(StandardCharsets.UTF_8);
	}

}