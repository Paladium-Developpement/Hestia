package fr.paladium.hestia.redis.json;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

import fr.paladium.hestia.redis.json.annotation.RedisJsonTransient;
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