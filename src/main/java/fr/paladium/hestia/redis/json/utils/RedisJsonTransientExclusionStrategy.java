package fr.paladium.hestia.redis.json.utils;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

import lombok.NonNull;

public final class RedisJsonTransientExclusionStrategy implements ExclusionStrategy {

	public static final RedisJsonTransientExclusionStrategy INSTANCE = new RedisJsonTransientExclusionStrategy();

	private RedisJsonTransientExclusionStrategy() {}

	@Override
	public boolean shouldSkipClass(final @NonNull Class<?> clazz) {
		return false;
	}

	@Override
	public boolean shouldSkipField(final @NonNull FieldAttributes field) {
		return field.getAnnotation(RedisJsonTransient.class) != null;
	}

}