package fr.paladium.hestia.redis.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class RedisCommandTest {

	@Test
	public void pingHasNoArgument() {
		assertEquals(0, RedisCommand.Base.ping().getArgs().length);
	}

	@Test
	public void setNxPxCarriesTheTtl() {
		assertArrayEquals(new String[] { "lock", "token", "NX", "PX", "5000" }, RedisCommand.Base.setNxPx("lock", "token", 5000L).getArgs());
	}

	@Test
	public void jsonMgetPutsThePathLast() {
		assertArrayEquals(new String[] { "a", "b", "$.version" }, RedisCommand.Json.mget("$.version", "a", "b").getArgs());
	}

	@Test
	public void argumentsWithSpacesAreQuoted() {
		assertEquals("SET key \"a b\"", RedisCommand.Base.set("key", "a b").toString());
	}

	@Test
	public void timeSeriesIncrByHasNoTimestamp() {
		assertArrayEquals(new String[] { "key", "2.0", "RETENTION", "1000", "LABELS", "type", "counter" }, RedisCommand.TimeSeries.incrBy("key", 2D, 1000L, "type", "counter").getArgs());
	}

	@Test
	public void jsonSetSerializesTheValueAtTheRoot() {
		final JsonObject value = new JsonObject();
		value.addProperty("balance", 5);
		assertArrayEquals(new String[] { "key", "$", "{\"balance\":5}" }, RedisCommand.Json.set(new Gson(), "key", value).getArgs());
	}

	@Test
	public void timeSeriesAddKeepsTheLastDuplicate() {
		assertArrayEquals(new String[] { "key", "*", "1.5", "RETENTION", "1000", "ON_DUPLICATE", "LAST", "LABELS", "type", "gauge" }, RedisCommand.TimeSeries.add("key", 1.5D, 1000L, "type", "gauge").getArgs());
	}

	@Test
	public void evalPrependsTheScriptAndTheKeyCount() {
		assertArrayEquals(new String[] { "script", "2", "a", "b", "c" }, RedisCommand.Base.eval("script", 2, "a", "b", "c").getArgs());
	}

	@Test
	public void sortedSearchesCarryTheSortAndTheLimit() {
		assertArrayEquals(new String[] { "idx:x", "*", "SORTBY", "score", "DESC", "LIMIT", "0", "10" }, RedisCommand.Search.search("idx:x", "*", "score", false, 0, 10).getArgs());
	}

	@Test
	public void timeSeriesCreateOnlyAddsLabelsWhenGiven() {
		assertArrayEquals(new String[] { "key", "RETENTION", "1000" }, RedisCommand.TimeSeries.create("key", 1000L).getArgs());
		assertArrayEquals(new String[] { "key", "RETENTION", "1000", "LABELS", "type", "gauge" }, RedisCommand.TimeSeries.create("key", 1000L, "type", "gauge").getArgs());
	}

	@Test
	public void responseCanBeReplacedWithoutTouchingArguments() {
		final RedisCommand command = RedisCommand.Json.get("key");
		final RedisCommand replaced = command.withResponse(RedisResponse.integer());
		assertSame(command.getArgs(), replaced.getArgs());
		assertEquals(3L, (long) replaced.<Long>parseResponse(3L));
	}

}