package fr.paladium.hestia.redis.json;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import fr.paladium.hestia.redis.impl.RedisCommand;
import lombok.NonNull;

public final class RedisJsonDiffResolver {

	public static void resolve(final @NonNull RedisJsonPatch patch, final @NonNull RedisJsonSchema schema, final @NonNull JsonElement snapshot, final @NonNull JsonElement current) {
		RedisJsonDiffResolver.resolve(patch, schema, "", RedisCommand.ROOT_PATH, snapshot, current);
	}

	private static boolean isSimpleIdentifier(final @NonNull String field) {
		if (field.isEmpty() || (field.charAt(0) >= '0' && field.charAt(0) <= '9')) {
			return false;
		}

		for (int i = 0; i < field.length(); i++) {
			final char c = field.charAt(i);
			if (!(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '_') {
				return false;
			}
		}
		return true;
	}

	private static boolean isPrimitiveArray(final @NonNull JsonArray array) {
		for (int i = 0; i < array.size(); i++) {
			if (!array.get(i).isJsonPrimitive()) {
				return false;
			}
		}
		return true;
	}

	private static @NonNull Set<JsonElement> toSet(final @NonNull JsonArray array) {
		final Set<JsonElement> set = new HashSet<>(array.size());
		for (int i = 0; i < array.size(); i++) {
			set.add(array.get(i));
		}
		return set;
	}

	private static boolean isPrefix(final @NonNull JsonArray snapshot, final @NonNull JsonArray current) {
		for (int i = 0; i < snapshot.size(); i++) {
			if (!snapshot.get(i).equals(current.get(i))) {
				return false;
			}
		}
		return true;
	}

	private static @NonNull String appendPath(final @NonNull String basePath, final @NonNull String field) {
		if (RedisJsonDiffResolver.isSimpleIdentifier(field)) {
			return basePath + "." + field;
		}
		return basePath + "[\"" + field.replace("\\", "\\\\").replace("\"", "\\\"") + "\"]";
	}

	private static void resolveArray(final @NonNull RedisJsonPatch patch, final @NonNull String parent, final @NonNull String path, final @NonNull JsonArray snapshot, final @NonNull JsonArray current) {
		if (current.size() > snapshot.size() && RedisJsonDiffResolver.isPrefix(snapshot, current)) {
			for (int i = snapshot.size(); i < current.size(); i++) {
				patch.append(path, parent, current.get(i), current);
			}
			return;
		}

		if (RedisJsonDiffResolver.isPrimitiveArray(snapshot) && RedisJsonDiffResolver.isPrimitiveArray(current)) {
			final Set<JsonElement> snapshotSet = RedisJsonDiffResolver.toSet(snapshot);
			final Set<JsonElement> currentSet = RedisJsonDiffResolver.toSet(current);
			if (snapshotSet.size() == snapshot.size() && currentSet.size() == current.size()) {
				for (final JsonElement element : snapshot) {
					if (!currentSet.contains(element)) {
						patch.remove(path, element);
					}
				}

				for (final JsonElement element : current) {
					if (!snapshotSet.contains(element)) {
						patch.append(path, parent, element, current);
					}
				}
				return;
			}
		}

		patch.set(path, parent, current);
	}

	private static void resolveObject(final @NonNull RedisJsonPatch patch, final @NonNull RedisJsonSchema schema, final @NonNull String path, final @NonNull JsonObject snapshot, final @NonNull JsonObject current) {
		for (final Map.Entry<String, JsonElement> entry : snapshot.entrySet()) {
			if (!current.has(entry.getKey())) {
				patch.delete(RedisJsonDiffResolver.appendPath(path, entry.getKey()));
			}
		}

		for (final Map.Entry<String, JsonElement> entry : current.entrySet()) {
			final String fieldPath = RedisJsonDiffResolver.appendPath(path, entry.getKey());
			if (!snapshot.has(entry.getKey())) {
				patch.set(fieldPath, path, entry.getValue());
			} else {
				RedisJsonDiffResolver.resolve(patch, schema.child(current, entry.getKey()), path, fieldPath, snapshot.get(entry.getKey()), entry.getValue());
			}
		}
	}

	private static void resolve(final @NonNull RedisJsonPatch patch, final @NonNull RedisJsonSchema schema, final @NonNull String parent, final @NonNull String path, final @NonNull JsonElement snapshot, final @NonNull JsonElement current) {
		if (snapshot.equals(current)) {
			return;
		}

		if (snapshot.getClass() != current.getClass()) {
			if (current.isJsonNull()) {
				patch.delete(path);
			} else {
				patch.set(path, parent, current);
			}
			return;
		}

		if (current.isJsonObject()) {
			RedisJsonDiffResolver.resolveObject(patch, schema, path, snapshot.getAsJsonObject(), current.getAsJsonObject());
		} else if (current.isJsonArray()) {
			RedisJsonDiffResolver.resolveArray(patch, parent, path, snapshot.getAsJsonArray(), current.getAsJsonArray());
		} else if (current.isJsonPrimitive()) {
			RedisJsonDiffResolver.resolvePrimitive(patch, schema, parent, path, snapshot.getAsJsonPrimitive(), current.getAsJsonPrimitive());
		}
	}

	private static void resolvePrimitive(final @NonNull RedisJsonPatch patch, final @NonNull RedisJsonSchema schema, final @NonNull String parent, final @NonNull String path, final @NonNull JsonPrimitive snapshot, final @NonNull JsonPrimitive current) {
		if (snapshot.isNumber() && current.isNumber() && !schema.isOverwrite()) {
			patch.increment(path, parent, current.getAsBigDecimal().subtract(snapshot.getAsBigDecimal()).stripTrailingZeros().toPlainString(), current);
			return;
		}

		patch.set(path, parent, current);
	}

}