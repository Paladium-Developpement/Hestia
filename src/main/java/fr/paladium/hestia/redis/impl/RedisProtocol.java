package fr.paladium.hestia.redis.impl;

import java.nio.charset.StandardCharsets;

import lombok.Getter;
import lombok.NonNull;
import redis.clients.jedis.commands.ProtocolCommand;

@Getter
public enum RedisProtocol implements ProtocolCommand {

	JSON_SET("JSON.SET"),
	JSON_GET("JSON.GET"),
	JSON_DEL("JSON.DEL"),
	JSON_NUMINCRBY("JSON.NUMINCRBY"),
	JSON_ARRAPPEND("JSON.ARRAPPEND"),
	JSON_ARRPOP("JSON.ARRPOP"),
	JSON_MGET("JSON.MGET"),

	SET("SET"),
	GET("GET"),
	EVAL("EVAL"),
	PING("PING"),
	PUBLISH("PUBLISH"),

	DEL("DEL"),
	EXISTS("EXISTS"),
	EXPIRE("EXPIRE"),
	PERSIST("PERSIST"),
	KEYS("KEYS"),
	SCAN("SCAN"),

	HSET("HSET"),
	HGET("HGET"),
	HDEL("HDEL"),
	HGETALL("HGETALL"),

	SADD("SADD"),
	SREM("SREM"),
	SMEMBERS("SMEMBERS"),

	LPUSH("LPUSH"),
	RPUSH("RPUSH"),
	LPOP("LPOP"),
	RPOP("RPOP"),
	LRANGE("LRANGE"),

	FT_CREATE("FT.CREATE"),
	FT_SEARCH("FT.SEARCH"),
	FT_DROPINDEX("FT.DROPINDEX"),

	TS_CREATE("TS.CREATE"),
	TS_ADD("TS.ADD"),
	TS_INCRBY("TS.INCRBY"),
	TS_MADD("TS.MADD");

	private final byte[] raw;

	private RedisProtocol(final @NonNull String command) {
		this.raw = command.getBytes(StandardCharsets.US_ASCII);
	}

}