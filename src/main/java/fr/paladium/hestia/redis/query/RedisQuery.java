package fr.paladium.hestia.redis.query;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.impl.RedisResponse;
import fr.paladium.hestia.redis.index.RedisIndexResolver;
import lombok.Getter;
import lombok.NonNull;

@Getter
public final class RedisQuery<T> {

	private final boolean list;
	private final Class<?> rootClass;
	private final Class<?> responseType;
	private final List<String> predicates = new ArrayList<>();

	private int limit = -1;
	private int offset = -1;
	private String sortField;
	private boolean ascending = true;

	private RedisQuery(final @NonNull Class<?> rootClass, final @NonNull Class<?> responseType, final boolean list) {
		this.rootClass = rootClass;
		this.responseType = responseType;
		this.list = list;
	}

	public static @NonNull <E> RedisQuery<E> single(final @NonNull Class<E> type) {
		return new RedisQuery<>(type, type, false);
	}

	public static @NonNull <E> RedisQuery<List<E>> list(final @NonNull Class<E> type) {
		return new RedisQuery<>(type, type, true);
	}

	public static @NonNull <E> RedisQuery<E> single(final @NonNull Class<E> responseType, final @NonNull Class<?> rootClass) {
		return new RedisQuery<>(rootClass, responseType, false);
	}

	public static @NonNull <E> RedisQuery<List<E>> list(final @NonNull Class<E> responseType, final @NonNull Class<?> rootClass) {
		return new RedisQuery<>(rootClass, responseType, true);
	}

	public @NonNull String cacheKey() {
		final StringBuilder builder = new StringBuilder("search:").append(RedisIndexResolver.indexName(this.rootClass));
		for (final String predicate : this.predicates) {
			builder.append(":").append(predicate);
		}

		if (this.sortField != null) {
			builder.append(":sort=").append(this.sortField).append(this.ascending ? "asc" : "desc");
		}

		if (this.limit > 0) {
			builder.append(":limit=").append(this.offset).append("-").append(this.limit);
		}

		return builder.toString();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public @NonNull RedisCommand toCommand(final @NonNull Gson gson) {
		final String index = RedisIndexResolver.indexName(this.rootClass);
		final String query = this.predicates.isEmpty() ? "*" : String.join(" ", this.predicates);
		final RedisCommand base = this.sortField != null && this.limit > 0 ? RedisCommand.Search.search(index, query, this.sortField, this.ascending, this.offset, this.limit) : RedisCommand.Search.search(index, query);
		return base.withResponse(this.list ? RedisResponse.searchJsonList(gson, (Class) this.responseType) : RedisResponse.searchJson(gson, (Class) this.responseType));
	}

	public @NonNull RedisQuery<T> limit(final int offset, final int limit) {
		this.offset = offset;
		this.limit = limit;
		return this;
	}

	public @NonNull RedisQuery<T> sort(final @NonNull String alias, final boolean ascending) {
		this.sortField = alias;
		this.ascending = ascending;
		return this;
	}

	public @NonNull RedisQuery<T> tag(final @NonNull String alias, final @NonNull String value) {
		this.predicates.add("@" + alias + ":{" + RedisQuery.escape(value) + "}");
		return this;
	}

	public @NonNull RedisQuery<T> range(final @NonNull String alias, final long min, final long max) {
		this.predicates.add("@" + alias + ":[" + min + " " + max + "]");
		return this;
	}

	public @NonNull <D, R> RedisQuery<T> tag(final @NonNull RedisGetter<D, R> getter, final @NonNull R value) {
		return this.tag(RedisQuery.resolveAlias(this.rootClass, getter), value.toString());
	}

	public @NonNull <D, R> RedisQuery<T> sort(final @NonNull RedisGetter<D, R> getter, final boolean ascending) {
		return this.sort(RedisQuery.resolveAlias(this.rootClass, getter), ascending);
	}

	public @NonNull <D, R extends Number> RedisQuery<T> range(final @NonNull RedisGetter<D, R> getter, final long min, final long max) {
		return this.range(RedisQuery.resolveAlias(this.rootClass, getter), min, max);
	}

	private static @NonNull String escape(final @NonNull String value) {
		final StringBuilder builder = new StringBuilder(value.length());
		for (int i = 0; i < value.length(); i++) {
			final char c = value.charAt(i);
			if (c == '-' || c == '.' || c == '@' || c == ':' || c == '\\' || c == ',' || c == ';' || c == '{' || c == '}' || c == '[' || c == ']' || c == '(' || c == ')' || c == '"' || c == '\'' || c == ' ') {
				builder.append('\\');
			}
			builder.append(c);
		}
		return builder.toString();
	}

	private static @NonNull String toFieldName(final @NonNull String methodName) {
		if (methodName.startsWith("get") && methodName.length() > 3) {
			return Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
		}

		if (methodName.startsWith("is") && methodName.length() > 2) {
			return Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3);
		}

		return methodName;
	}

	private static @NonNull String resolveAlias(final @NonNull Class<?> rootClass, final @NonNull RedisGetter<?, ?> getter) {
		try {
			final Method writeReplace = getter.getClass().getDeclaredMethod("writeReplace");
			writeReplace.setAccessible(true);
			final SerializedLambda lambda = (SerializedLambda) writeReplace.invoke(getter);
			final Class<?> ownerClass = Class.forName(lambda.getImplClass().replace('/', '.'));
			return RedisIndexResolver.aliasFor(rootClass, ownerClass, RedisQuery.toFieldName(lambda.getImplMethodName()));
		} catch (final ReflectiveOperationException exception) {
			throw new IllegalArgumentException("Failed to resolve getter alias: " + exception.getMessage(), exception);
		}
	}

}