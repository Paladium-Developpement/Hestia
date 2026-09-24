package fr.paladium.hestia.redis.json;

import com.google.gson.JsonElement;

import lombok.NonNull;

@FunctionalInterface
public interface RedisJsonTypeResolver {

	public static final RedisJsonTypeResolver IDENTITY = (type, json) -> type;

	public @NonNull Class<?> resolve(final @NonNull Class<?> type, final JsonElement json);

}