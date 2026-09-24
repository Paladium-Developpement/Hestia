package fr.paladium.hestia.redis.impl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import lombok.NonNull;

@FunctionalInterface
public interface RedisResponse<T> {

	public T parse(final Object raw);

	@SuppressWarnings("unchecked")
	public static @NonNull <T> RedisResponse<T> raw() {
		return raw -> (T) raw;
	}

	public static @NonNull RedisResponse<Long> integer() {
		return raw -> raw == null ? null : (Long) raw;
	}

	public static @NonNull RedisResponse<Boolean> bool() {
		return raw -> raw != null && (Long) raw > 0;
	}

	public static @NonNull RedisResponse<String> string() {
		return raw -> raw == null ? null : new String((byte[]) raw, StandardCharsets.UTF_8);
	}

	public static @NonNull RedisResponse<String> status() {
		return raw -> raw == null ? null : raw.toString();
	}

	public static @NonNull RedisResponse<String> jsonString() {
		return RedisResponse.string();
	}

	public static @NonNull RedisResponse<List<String>> stringList() {
		return raw -> {
			if (raw == null) {
				return new ArrayList<>();
			}

			final List<?> list = (List<?>) raw;
			final List<String> result = new ArrayList<>(list.size());
			for (final Object item : list) {
				result.add(item == null ? null : new String((byte[]) item, StandardCharsets.UTF_8));
			}
			return result;
		};
	}

	public static @NonNull RedisResponse<String> searchJsonString() {
		return raw -> {
			final List<String> results = RedisResponse.searchJsonStringList().parse(raw);
			return results.isEmpty() ? null : results.get(0);
		};
	}

	public static @NonNull RedisResponse<List<String>> jsonStringList() {
		return raw -> {
			if (raw == null) {
				return new ArrayList<>();
			}

			final List<?> list = (List<?>) raw;
			final List<String> result = new ArrayList<>(list.size());
			for (final Object item : list) {
				if (item == null) {
					continue;
				}

				final String json = new String((byte[]) item, StandardCharsets.UTF_8);
				result.add(json.startsWith("[") ? json.substring(1, json.length() - 1) : json);
			}
			return result;
		};
	}

	@SuppressWarnings("unchecked")
	public static @NonNull RedisResponse<List<String>> searchJsonStringList() {
		return raw -> {
			final List<String> result = new ArrayList<>();
			if (!(raw instanceof List)) {
				return result;
			}

			final List<Object> searchResult = (List<Object>) raw;
			if (searchResult.isEmpty() || (Long) searchResult.get(0) == 0L) {
				return result;
			}

			for (int i = 1; i < searchResult.size(); i += 2) {
				final List<Object> fields = (List<Object>) searchResult.get(i + 1);
				final String json = new String((byte[]) fields.get(1), StandardCharsets.UTF_8);
				result.add(json.startsWith("[") ? json.substring(1, json.length() - 1) : json);
			}
			return result;
		};
	}

	public static @NonNull <T> RedisResponse<T> json(final @NonNull Gson gson, final @NonNull Class<T> type) {
		return raw -> raw == null ? null : gson.fromJson(new String((byte[]) raw, StandardCharsets.UTF_8), type);
	}

	public static @NonNull <T> RedisResponse<T> searchJson(final @NonNull Gson gson, final @NonNull Class<T> type) {
		return raw -> {
			final String json = RedisResponse.searchJsonString().parse(raw);
			return json == null ? null : gson.fromJson(json, type);
		};
	}

	public static @NonNull <T> RedisResponse<List<T>> jsonList(final @NonNull Gson gson, final @NonNull Class<T> type) {
		return raw -> {
			final List<String> jsonList = RedisResponse.jsonStringList().parse(raw);
			final List<T> result = new ArrayList<>(jsonList.size());
			for (final String json : jsonList) {
				result.add(gson.fromJson(json, type));
			}
			return result;
		};
	}

	public static @NonNull <T> RedisResponse<List<T>> searchJsonList(final @NonNull Gson gson, final @NonNull Class<T> type) {
		return raw -> {
			final List<String> jsonList = RedisResponse.searchJsonStringList().parse(raw);
			final List<T> result = new ArrayList<>(jsonList.size());
			for (final String json : jsonList) {
				result.add(gson.fromJson(json, type));
			}
			return result;
		};
	}

}