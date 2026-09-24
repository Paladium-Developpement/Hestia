package fr.paladium.hestia.redis.impl;

import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;

import lombok.Getter;
import lombok.NonNull;

@Getter
public final class RedisCommand {

	public static final String ROOT_PATH = "$";

	private final String[] args;
	private final RedisProtocol command;
	private final RedisResponse<?> response;

	private RedisCommand(final @NonNull RedisProtocol command, final @NonNull String... args) {
		this(command, null, args);
	}

	private RedisCommand(final @NonNull RedisProtocol command, final RedisResponse<?> response, final @NonNull String... args) {
		this.command = command;
		this.args = args;
		this.response = response;
	}

	@SuppressWarnings("unchecked")
	public <T> T parseResponse(final Object raw) {
		if (this.response == null) {
			return (T) raw;
		}
		return (T) this.response.parse(raw);
	}

	public @NonNull RedisCommand withResponse(final @NonNull RedisResponse<?> response) {
		return new RedisCommand(this.command, response, this.args);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(new String(this.command.getRaw(), StandardCharsets.US_ASCII));
		for (final String arg : this.args) {
			builder.append(' ');
			if (arg.isEmpty() || arg.indexOf(' ') >= 0 || arg.indexOf('"') >= 0) {
				builder.append('"').append(arg.replace("\"", "\\\"")).append('"');
			} else {
				builder.append(arg);
			}
		}
		return builder.toString();
	}

	public static class Json {

		public static @NonNull RedisCommand get(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.JSON_GET, RedisResponse.jsonString(), key);
		}

		public static @NonNull RedisCommand del(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.JSON_DEL, key);
		}

		public static @NonNull RedisCommand get(final @NonNull String key, final @NonNull String path) {
			return new RedisCommand(RedisProtocol.JSON_GET, RedisResponse.jsonString(), key, path);
		}

		public static @NonNull RedisCommand del(final @NonNull String key, final @NonNull String path) {
			return new RedisCommand(RedisProtocol.JSON_DEL, key, path);
		}

		public static @NonNull RedisCommand remove(final @NonNull String key, final @NonNull String path) {
			return new RedisCommand(RedisProtocol.JSON_ARRPOP, key, path, "-1");
		}

		public static @NonNull RedisCommand mget(final @NonNull String path, final @NonNull String... keys) {
			final String[] args = new String[keys.length + 1];
			System.arraycopy(keys, 0, args, 0, keys.length);
			args[keys.length] = path;
			return new RedisCommand(RedisProtocol.JSON_MGET, RedisResponse.jsonStringList(), args);
		}

		public static @NonNull RedisCommand remove(final @NonNull String key, final @NonNull String path, final int index) {
			return new RedisCommand(RedisProtocol.JSON_ARRPOP, key, path, String.valueOf(index));
		}

		public static @NonNull RedisCommand incrBy(final @NonNull String key, final @NonNull String path, final long value) {
			return new RedisCommand(RedisProtocol.JSON_NUMINCRBY, key, path, String.valueOf(value));
		}

		public static @NonNull RedisCommand incrBy(final @NonNull String key, final @NonNull String path, final double value) {
			return new RedisCommand(RedisProtocol.JSON_NUMINCRBY, key, path, String.valueOf(value));
		}

		public static @NonNull RedisCommand set(final @NonNull Gson gson, final @NonNull String key, final @NonNull Object value) {
			return new RedisCommand(RedisProtocol.JSON_SET, key, RedisCommand.ROOT_PATH, gson.toJson(value));
		}

		public static @NonNull RedisCommand set(final @NonNull Gson gson, final @NonNull String key, final @NonNull String path, final @NonNull Object value) {
			return new RedisCommand(RedisProtocol.JSON_SET, key, path, gson.toJson(value));
		}

		public static @NonNull RedisCommand append(final @NonNull Gson gson, final @NonNull String key, final @NonNull String path, final @NonNull Object... values) {
			final String[] args = new String[values.length + 2];
			args[0] = key;
			args[1] = path;
			for (int i = 0; i < values.length; i++) {
				args[i + 2] = gson.toJson(values[i]);
			}
			return new RedisCommand(RedisProtocol.JSON_ARRAPPEND, args);
		}

	}

	public static class Base {

		public static @NonNull RedisCommand ping() {
			return new RedisCommand(RedisProtocol.PING, RedisResponse.status());
		}

		public static @NonNull RedisCommand get(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.GET, RedisResponse.string(), key);
		}

		public static @NonNull RedisCommand del(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.DEL, key);
		}

		public static @NonNull RedisCommand lpop(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.LPOP, RedisResponse.string(), key);
		}

		public static @NonNull RedisCommand rpop(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.RPOP, RedisResponse.string(), key);
		}

		public static @NonNull RedisCommand exists(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.EXISTS, RedisResponse.bool(), key);
		}

		public static @NonNull RedisCommand persist(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.PERSIST, key);
		}

		public static @NonNull RedisCommand hgetall(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.HGETALL, RedisResponse.stringList(), key);
		}

		public static @NonNull RedisCommand smembers(final @NonNull String key) {
			return new RedisCommand(RedisProtocol.SMEMBERS, RedisResponse.stringList(), key);
		}

		public static @NonNull RedisCommand expire(final @NonNull String key, final long seconds) {
			return new RedisCommand(RedisProtocol.EXPIRE, key, String.valueOf(seconds));
		}

		public static @NonNull RedisCommand set(final @NonNull String key, final @NonNull String value) {
			return new RedisCommand(RedisProtocol.SET, key, value);
		}

		public static @NonNull RedisCommand hget(final @NonNull String key, final @NonNull String field) {
			return new RedisCommand(RedisProtocol.HGET, RedisResponse.string(), key, field);
		}

		public static @NonNull RedisCommand hdel(final @NonNull String key, final @NonNull String... fields) {
			return new RedisCommand(RedisProtocol.HDEL, RedisCommand.Base.prepend(key, fields));
		}

		public static @NonNull RedisCommand sadd(final @NonNull String key, final @NonNull String... members) {
			return new RedisCommand(RedisProtocol.SADD, RedisCommand.Base.prepend(key, members));
		}

		public static @NonNull RedisCommand srem(final @NonNull String key, final @NonNull String... members) {
			return new RedisCommand(RedisProtocol.SREM, RedisCommand.Base.prepend(key, members));
		}

		public static @NonNull RedisCommand lpush(final @NonNull String key, final @NonNull String... values) {
			return new RedisCommand(RedisProtocol.LPUSH, RedisCommand.Base.prepend(key, values));
		}

		public static @NonNull RedisCommand rpush(final @NonNull String key, final @NonNull String... values) {
			return new RedisCommand(RedisProtocol.RPUSH, RedisCommand.Base.prepend(key, values));
		}

		public static @NonNull RedisCommand lrange(final @NonNull String key, final long start, final long stop) {
			return new RedisCommand(RedisProtocol.LRANGE, RedisResponse.stringList(), key, String.valueOf(start), String.valueOf(stop));
		}

		public static @NonNull RedisCommand publish(final @NonNull String channel, final @NonNull String message) {
			return new RedisCommand(RedisProtocol.PUBLISH, RedisResponse.integer(), channel, message);
		}

		public static @NonNull RedisCommand setNxPx(final @NonNull String key, final @NonNull String value, final long ttlMs) {
			return new RedisCommand(RedisProtocol.SET, RedisResponse.string(), key, value, "NX", "PX", String.valueOf(ttlMs));
		}

		public static @NonNull RedisCommand hset(final @NonNull String key, final @NonNull String field, final @NonNull String value) {
			return new RedisCommand(RedisProtocol.HSET, key, field, value);
		}

		public static @NonNull RedisCommand eval(final @NonNull String script, final int keyCount, final @NonNull String... keysAndArgs) {
			final String[] args = new String[keysAndArgs.length + 2];
			args[0] = script;
			args[1] = String.valueOf(keyCount);
			System.arraycopy(keysAndArgs, 0, args, 2, keysAndArgs.length);
			return new RedisCommand(RedisProtocol.EVAL, args);
		}

		private static @NonNull String[] prepend(final @NonNull String first, final @NonNull String... values) {
			final String[] args = new String[values.length + 1];
			args[0] = first;
			System.arraycopy(values, 0, args, 1, values.length);
			return args;
		}

	}

	public static class Search {

		public static @NonNull RedisCommand drop(final @NonNull String index) {
			return new RedisCommand(RedisProtocol.FT_DROPINDEX, index);
		}

		public static @NonNull RedisCommand search(final @NonNull String index, final @NonNull String query) {
			return new RedisCommand(RedisProtocol.FT_SEARCH, RedisResponse.searchJsonString(), index, query);
		}

		public static @NonNull RedisCommand create(final @NonNull String index, final @NonNull String... args) {
			final String[] fullArgs = new String[args.length + 1];
			fullArgs[0] = index;
			System.arraycopy(args, 0, fullArgs, 1, args.length);
			return new RedisCommand(RedisProtocol.FT_CREATE, fullArgs);
		}

		public static @NonNull RedisCommand search(final @NonNull String index, final @NonNull String query, final @NonNull String sortBy, final boolean ascending, final int offset, final int limit) {
			return new RedisCommand(RedisProtocol.FT_SEARCH, RedisResponse.searchJsonStringList(), index, query, "SORTBY", sortBy, ascending ? "ASC" : "DESC", "LIMIT", String.valueOf(offset), String.valueOf(limit));
		}

	}

	public static class TimeSeries {

		public static @NonNull RedisCommand create(final @NonNull String key, final long retentionMs, final @NonNull String... labels) {
			final int base = 3;
			final int labelsSize = labels.length == 0 ? 0 : labels.length + 1;
			final String[] args = new String[base + labelsSize];
			args[0] = key;
			args[1] = "RETENTION";
			args[2] = String.valueOf(retentionMs);
			if (labels.length > 0) {
				args[base] = "LABELS";
				System.arraycopy(labels, 0, args, base + 1, labels.length);
			}
			return new RedisCommand(RedisProtocol.TS_CREATE, args);
		}

		public static @NonNull RedisCommand add(final @NonNull String key, final double value, final long retentionMs, final @NonNull String... labels) {
			final String[] args = RedisCommand.TimeSeries.buildArgs(key, "*", String.valueOf(value), retentionMs, "ON_DUPLICATE", "LAST", labels);
			return new RedisCommand(RedisProtocol.TS_ADD, args);
		}

		public static @NonNull RedisCommand incrBy(final @NonNull String key, final double value, final long retentionMs, final @NonNull String... labels) {
			final String[] args = RedisCommand.TimeSeries.buildArgs(key, null, String.valueOf(value), retentionMs, null, null, labels);
			return new RedisCommand(RedisProtocol.TS_INCRBY, args);
		}

		private static @NonNull String[] buildArgs(final @NonNull String key, final String timestamp, final @NonNull String value, final long retentionMs, final String duplicateKey, final String duplicateValue, final @NonNull String... labels) {
			int size = 2;
			if (timestamp != null) {
				size += 1;
			}

			size += 2;
			if (duplicateKey != null) {
				size += 2;
			}

			if (labels.length > 0) {
				size += labels.length + 1;
			}

			final String[] args = new String[size];
			int i = 0;
			args[i++] = key;
			if (timestamp != null) {
				args[i++] = timestamp;
			}

			args[i++] = value;
			args[i++] = "RETENTION";
			args[i++] = String.valueOf(retentionMs);
			if (duplicateKey != null) {
				args[i++] = duplicateKey;
				args[i++] = duplicateValue;
			}

			if (labels.length > 0) {
				args[i++] = "LABELS";
				System.arraycopy(labels, 0, args, i, labels.length);
			}

			return args;
		}

	}

}