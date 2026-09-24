package fr.paladium.hestia.redis.json;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.json.annotation.RedisJsonSnapshot;
import fr.paladium.hestia.redis.json.annotation.RedisJsonVersion;
import lombok.Getter;
import lombok.NonNull;

@Getter
public final class RedisJsonSerializer {

	private static final Field NO_FIELD;
	private static final Map<Class<?>, Field> VERSION_FIELDS = new ConcurrentHashMap<>();
	private static final Map<Class<?>, Field> SNAPSHOT_FIELDS = new ConcurrentHashMap<>();

	static {
		try {
			NO_FIELD = RedisJsonSerializer.class.getDeclaredField("NO_FIELD");
		} catch (final NoSuchFieldException exception) {
			throw new ExceptionInInitializerError(exception);
		}
	}

	private final Gson gson;
	private final RedisJsonTypeResolver typeResolver;
	private final Map<String, JsonElement> snapshots = new ConcurrentHashMap<>();

	private RedisJsonSerializer(final @NonNull Gson gson, final @NonNull RedisJsonTypeResolver typeResolver) {
		this.gson = gson;
		this.typeResolver = typeResolver;
	}

	public static @NonNull RedisJsonSerializer create(final @NonNull Gson gson, final @NonNull RedisJsonTypeResolver typeResolver) {
		return new RedisJsonSerializer(gson, typeResolver);
	}

	public long getVersion(final @NonNull Object object) {
		final Field versionField = RedisJsonSerializer.resolveVersionField(object.getClass());
		if (versionField == null) {
			return 0L;
		}

		try {
			return versionField.getLong(object);
		} catch (final IllegalAccessException exception) {
			throw new IllegalStateException(exception);
		}
	}

	public boolean hasSnapshot(final @NonNull String key) {
		return this.snapshots.containsKey(key);
	}

	public void removeSnapshot(final @NonNull String key) {
		this.snapshots.remove(key);
	}

	public @NonNull JsonElement capture(final @NonNull Object object) {
		final JsonElement tree = this.gson.toJsonTree(object);
		if (tree == null || !tree.isJsonObject()) {
			throw new IllegalArgumentException("Object cannot be serialized to a JSON object: " + object.getClass().getName());
		}
		return tree;
	}

	public void bind(final @NonNull Object object, final @NonNull JsonElement tree) {
		final Field snapshotField = RedisJsonSerializer.resolveSnapshotField(object.getClass());
		if (snapshotField == null) {
			return;
		}

		try {
			snapshotField.set(object, tree);
		} catch (final IllegalAccessException exception) {
			throw new IllegalStateException(exception);
		}
	}

	public @NonNull RedisJsonSerializer snapshot(final @NonNull String key, final @NonNull Object object) {
		this.setSnapshot(key, this.gson.toJsonTree(object), object.getClass());
		return this;
	}

	public @NonNull RedisJsonSerializer snapshot(final @NonNull String key, final @NonNull String json, final @NonNull Class<?> clazz) {
		this.setSnapshot(key, JsonParser.parseString(json), clazz);
		return this;
	}

	public @NonNull RedisJsonSerializer snapshot(final @NonNull String key, final @NonNull JsonElement element, final @NonNull Class<?> clazz) {
		this.setSnapshot(key, element, clazz);
		return this;
	}

	public @NonNull RedisJsonSerializeResult prepare(final @NonNull String key, final @NonNull Object object, final @NonNull JsonElement captured) {
		final Class<?> clazz = object.getClass();
		final Field versionField = RedisJsonSerializer.resolveVersionField(clazz);
		final Field snapshotField = RedisJsonSerializer.resolveSnapshotField(clazz);

		JsonElement objectSnapshot = null;
		if (snapshotField != null) {
			try {
				objectSnapshot = (JsonElement) snapshotField.get(object);
			} catch (final IllegalAccessException exception) {
				throw new IllegalStateException(exception);
			}
		}

		final JsonElement snapshot = objectSnapshot != null ? objectSnapshot : this.snapshots.get(key);
		final JsonObject current = captured.getAsJsonObject();
		final long version = versionField == null ? 0L : RedisJsonSerializer.nextVersion(versionField.getName(), snapshot, current);
		if (versionField != null) {
			current.addProperty(versionField.getName(), version);
		}

		final RedisJsonPatch patch = RedisJsonPatch.create(key, this.gson);
		if (snapshot == null) {
			patch.set(RedisCommand.ROOT_PATH, "", current);
		} else {
			RedisJsonDiffResolver.resolve(patch, RedisJsonSchema.of(clazz, this.typeResolver), snapshot, current);
		}

		final Runnable commit = () -> {
			synchronized (object) {
				try {
					if (versionField != null) {
						versionField.setLong(object, version);
					}

					if (snapshotField != null) {
						snapshotField.set(object, current);
					}
				} catch (final IllegalAccessException exception) {
					throw new IllegalStateException(exception);
				}
			}
			this.setSnapshot(key, current, clazz);
		};

		return new RedisJsonSerializeResult(patch, commit);
	}

	private void setSnapshot(final @NonNull String key, final @NonNull JsonElement element, final @NonNull Class<?> clazz) {
		final Field versionField = RedisJsonSerializer.resolveVersionField(clazz);
		final String versionFieldName = versionField != null ? versionField.getName() : null;
		this.snapshots.compute(key, (k, current) -> {
			if (versionFieldName != null && current != null && RedisJsonSerializer.extractVersion(current, versionFieldName) > RedisJsonSerializer.extractVersion(element, versionFieldName)) {
				return current;
			}
			return element;
		});
	}

	public static Field resolveVersionField(final @NonNull Class<?> clazz) {
		return RedisJsonSerializer.resolveField(RedisJsonSerializer.VERSION_FIELDS, clazz, RedisJsonVersion.class, long.class);
	}

	private static Field resolveSnapshotField(final @NonNull Class<?> clazz) {
		return RedisJsonSerializer.resolveField(RedisJsonSerializer.SNAPSHOT_FIELDS, clazz, RedisJsonSnapshot.class, JsonElement.class);
	}

	private static long extractVersion(final JsonElement element, final @NonNull String fieldName) {
		if (element == null || !element.isJsonObject()) {
			return Long.MIN_VALUE;
		}

		final JsonElement value = element.getAsJsonObject().get(fieldName);
		if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
			return Long.MIN_VALUE;
		}

		return value.getAsLong();
	}

	private static long nextVersion(final @NonNull String fieldName, final JsonElement snapshot, final @NonNull JsonElement current) {
		long version = RedisJsonSerializer.extractVersion(snapshot, fieldName);
		if (version == Long.MIN_VALUE) {
			version = RedisJsonSerializer.extractVersion(current, fieldName);
		}
		return version == Long.MIN_VALUE ? 1L : version + 1L;
	}

	private static Field resolveField(final @NonNull Map<Class<?>, Field> cache, final @NonNull Class<?> clazz, final @NonNull Class<? extends Annotation> annotation, final @NonNull Class<?> type) {
		final Field cached = cache.get(clazz);
		if (cached != null) {
			return cached == RedisJsonSerializer.NO_FIELD ? null : cached;
		}

		Class<?> current = clazz;
		while (current != null && current != Object.class) {
			for (final Field field : current.getDeclaredFields()) {
				if (!field.isAnnotationPresent(annotation)) {
					continue;
				}

				if (field.getType() != type) {
					throw new IllegalArgumentException("@" + annotation.getSimpleName() + " must be on a " + type.getSimpleName() + " field, got " + field.getType().getName() + " on " + clazz.getName() + "." + field.getName());
				}

				field.setAccessible(true);
				cache.put(clazz, field);
				return field;
			}
			current = current.getSuperclass();
		}

		cache.put(clazz, RedisJsonSerializer.NO_FIELD);
		return null;
	}

}