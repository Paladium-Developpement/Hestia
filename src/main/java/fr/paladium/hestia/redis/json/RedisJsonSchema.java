package fr.paladium.hestia.redis.json;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.JsonElement;
import com.google.gson.annotations.SerializedName;

import fr.paladium.hestia.redis.json.annotation.RedisJsonOverwrite;
import lombok.Getter;
import lombok.NonNull;

@Getter
public final class RedisJsonSchema {

	private static final Map<Class<?>, Map<String, Field>> FIELDS = new ConcurrentHashMap<>();
	private static final RedisJsonSchema UNKNOWN = new RedisJsonSchema(null, false, RedisJsonTypeResolver.IDENTITY);
	private static final RedisJsonSchema OVERWRITE = new RedisJsonSchema(null, true, RedisJsonTypeResolver.IDENTITY);

	private final Type type;
	private final boolean overwrite;
	private final RedisJsonTypeResolver resolver;

	private RedisJsonSchema(final Type type, final boolean overwrite, final @NonNull RedisJsonTypeResolver resolver) {
		this.type = type;
		this.overwrite = overwrite;
		this.resolver = resolver;
	}

	public static @NonNull RedisJsonSchema of(final @NonNull Class<?> root, final @NonNull RedisJsonTypeResolver resolver) {
		return new RedisJsonSchema(root, false, resolver);
	}

	public @NonNull RedisJsonSchema child(final @NonNull JsonElement node, final @NonNull String key) {
		final Class<?> raw = RedisJsonSchema.raw(this.type);
		if (raw == null) {
			return this.overwrite ? RedisJsonSchema.OVERWRITE : RedisJsonSchema.UNKNOWN;
		}

		if (Map.class.isAssignableFrom(raw)) {
			return new RedisJsonSchema(RedisJsonSchema.argument(this.type, 1), this.overwrite, this.resolver);
		}

		final Field field = RedisJsonSchema.fields(this.resolver.resolve(raw, node)).get(key);
		if (field == null) {
			return this.overwrite ? RedisJsonSchema.OVERWRITE : RedisJsonSchema.UNKNOWN;
		}

		return new RedisJsonSchema(field.getGenericType(), this.overwrite || field.isAnnotationPresent(RedisJsonOverwrite.class), this.resolver);
	}

	private static Class<?> raw(final Type type) {
		if (type instanceof Class) {
			return (Class<?>) type;
		}

		if (type instanceof ParameterizedType) {
			return (Class<?>) ((ParameterizedType) type).getRawType();
		}

		return null;
	}

	private static Type argument(final Type type, final int index) {
		if (!(type instanceof ParameterizedType)) {
			return null;
		}

		final Type[] arguments = ((ParameterizedType) type).getActualTypeArguments();
		return index < arguments.length ? arguments[index] : null;
	}

	private static @NonNull Map<String, Field> fields(final @NonNull Class<?> clazz) {
		return RedisJsonSchema.FIELDS.computeIfAbsent(clazz, key -> {
			final Map<String, Field> fields = new HashMap<>();
			Class<?> current = key;
			while (current != null && current != Object.class) {
				for (final Field field : current.getDeclaredFields()) {
					if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
						continue;
					}

					final SerializedName serializedName = field.getAnnotation(SerializedName.class);
					fields.putIfAbsent(serializedName == null ? field.getName() : serializedName.value(), field);
				}
				current = current.getSuperclass();
			}
			return fields;
		});
	}

}