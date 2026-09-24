package fr.paladium.hestia.redis.index;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import fr.paladium.hestia.redis.impl.RedisCommand;
import lombok.NonNull;

public final class RedisIndexResolver {

	private static final Map<Class<?>, Map<String, String>> ALIASES = new ConcurrentHashMap<>();

	public static RedisCommand resolve(final @NonNull Class<?> clazz) {
		final List<String> schema = new ArrayList<>();
		final Map<String, String> aliases = new HashMap<>();
		RedisIndexResolver.analyzeClass(clazz, RedisCommand.ROOT_PATH, schema, new HashSet<>(), RedisIndexResolver.getRootPackage(clazz), aliases);
		if (schema.isEmpty()) {
			return null;
		}

		RedisIndexResolver.ALIASES.put(clazz, aliases);
		final List<String> args = new ArrayList<>();
		args.add("ON");
		args.add("JSON");
		args.add("PREFIX");
		args.add("1");
		args.add(RedisIndexResolver.prefix(clazz));
		args.add("SCHEMA");
		args.addAll(schema);
		return RedisCommand.Search.create(RedisIndexResolver.indexName(clazz), args.toArray(new String[0]));
	}

	public static @NonNull String toAlias(final @NonNull String path) {
		return path.replace("$.", "").replace(".*.", "_").replace(".*", "").replace("[*].", "_").replace("[*]", "").replace(".", "_");
	}

	public static @NonNull String prefix(final @NonNull Class<?> clazz) {
		return clazz.getSimpleName().toLowerCase() + ":";
	}

	public static @NonNull String indexName(final @NonNull Class<?> clazz) {
		return "idx:" + clazz.getSimpleName().toLowerCase();
	}

	public static @NonNull String aliasFor(final @NonNull Class<?> rootClass, final @NonNull Class<?> ownerClass, final @NonNull String fieldName) {
		final Map<String, String> rootAliases = RedisIndexResolver.ALIASES.get(rootClass);
		if (rootAliases == null) {
			throw new IllegalArgumentException("No index registered for " + rootClass.getName() + " (did you forget RedisClient.index(" + rootClass.getSimpleName() + ".class)?)");
		}

		final String alias = rootAliases.get(ownerClass.getName() + "." + fieldName);
		if (alias == null) {
			throw new IllegalArgumentException("Field " + ownerClass.getSimpleName() + "." + fieldName + " is not indexed under " + rootClass.getSimpleName() + " (missing @RedisIndex?)");
		}

		return alias;
	}

	private static boolean isLeafType(final @NonNull Class<?> type) {
		return type.isPrimitive() || Number.class.isAssignableFrom(type) || type == String.class || type == Boolean.class || type == Character.class || type == UUID.class || type.isEnum();
	}

	private static @NonNull String detectType(final @NonNull Class<?> type) {
		if (Number.class.isAssignableFrom(type)) {
			return "NUMERIC";
		}

		if (type == int.class || type == long.class || type == double.class || type == float.class || type == short.class || type == byte.class) {
			return "NUMERIC";
		}

		return "TAG";
	}

	private static @NonNull String getRootPackage(final @NonNull Class<?> clazz) {
		final String[] parts = clazz.getPackage().getName().split("\\.");
		if (parts.length <= 3) {
			return clazz.getPackage().getName();
		}
		return parts[0] + "." + parts[1] + "." + parts[2];
	}

	private static @NonNull List<Field> getAllFields(final @NonNull Class<?> clazz) {
		final List<Field> fields = new ArrayList<>();
		Class<?> current = clazz;
		while (current != null && current != Object.class) {
			Collections.addAll(fields, current.getDeclaredFields());
			current = current.getSuperclass();
		}
		return fields;
	}

	private static Class<?> getGenericType(final @NonNull Field field, final int index) {
		if (!(field.getGenericType() instanceof ParameterizedType)) {
			return null;
		}

		final Type[] typeArgs = ((ParameterizedType) field.getGenericType()).getActualTypeArguments();
		if (index >= typeArgs.length) {
			return null;
		}

		if (typeArgs[index] instanceof Class) {
			return (Class<?>) typeArgs[index];
		}

		return null;
	}

	private static @NonNull String resolveMapPath(final @NonNull Class<?> impl, final @NonNull String mapPath) {
		for (final Field field : RedisIndexResolver.getAllFields(impl)) {
			if (!field.isAnnotationPresent(RedisIndex.class)) {
				continue;
			}

			final String value = field.getAnnotation(RedisIndex.class).value();
			if (!value.isEmpty()) {
				return mapPath + "." + value;
			}
		}
		return mapPath + ".*";
	}

	private static void addSchemaField(final @NonNull String fieldPath, final @NonNull Class<?> fieldType, final @NonNull List<String> schema, final boolean sortable) {
		schema.add(fieldPath);
		schema.add("AS");
		schema.add(RedisIndexResolver.toAlias(fieldPath));
		final String type = RedisIndexResolver.detectType(fieldType);
		schema.add(type);
		if ("TAG".equals(type)) {
			schema.add("SEPARATOR");
			schema.add(";");
		}

		if (sortable) {
			schema.add("SORTABLE");
		}
	}

	private static void analyzeClass(final @NonNull Class<?> clazz, final @NonNull String path, final @NonNull List<String> schema, final @NonNull Set<Class<?>> visited, final @NonNull String rootPackage, final @NonNull Map<String, String> aliases) {
		if (!visited.add(clazz)) {
			return;
		}

		for (final Field field : RedisIndexResolver.getAllFields(clazz)) {
			if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
				continue;
			}

			final String fieldPath = path + "." + field.getName();
			if (field.isAnnotationPresent(RedisIndex.class)) {
				final RedisIndex annotation = field.getAnnotation(RedisIndex.class);
				RedisIndexResolver.addSchemaField(fieldPath, field.getType(), schema, annotation.sortable());
				aliases.putIfAbsent(clazz.getName() + "." + field.getName(), RedisIndexResolver.toAlias(fieldPath));
			}

			if (Map.class.isAssignableFrom(field.getType())) {
				final Class<?> valueType = RedisIndexResolver.getGenericType(field, 1);
				if (valueType != null && valueType.isInterface()) {
					RedisIndexResolver.analyzeInterfaceMap(valueType, fieldPath, schema, visited, rootPackage, aliases);
				} else if (valueType != null && !RedisIndexResolver.isLeafType(valueType)) {
					RedisIndexResolver.analyzeClass(valueType, fieldPath + ".*", schema, new HashSet<>(visited), rootPackage, aliases);
				}
			} else if (Collection.class.isAssignableFrom(field.getType())) {
				final Class<?> elementType = RedisIndexResolver.getGenericType(field, 0);
				if (elementType != null && !RedisIndexResolver.isLeafType(elementType)) {
					RedisIndexResolver.analyzeClass(elementType, fieldPath + "[*]", schema, new HashSet<>(visited), rootPackage, aliases);
				}
			} else if (!RedisIndexResolver.isLeafType(field.getType()) && !field.isAnnotationPresent(RedisIndex.class)) {
				RedisIndexResolver.analyzeClass(field.getType(), fieldPath, schema, new HashSet<>(visited), rootPackage, aliases);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static void analyzeInterfaceMap(final @NonNull Class<?> iface, final @NonNull String mapPath, final @NonNull List<String> schema, final @NonNull Set<Class<?>> visited, final @NonNull String rootPackage, final @NonNull Map<String, String> aliases) {
		final Reflections reflections = new Reflections(new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage(rootPackage)).setScanners(new SubTypesScanner()));
		final Set<? extends Class<?>> subtypes = reflections.getSubTypesOf((Class<Object>) iface);
		for (final Class<?> impl : subtypes) {
			if (impl.isInterface() || Modifier.isAbstract(impl.getModifiers())) {
				continue;
			}

			RedisIndexResolver.analyzeClass(impl, RedisIndexResolver.resolveMapPath(impl, mapPath), schema, new HashSet<>(visited), rootPackage, aliases);
		}
	}

}