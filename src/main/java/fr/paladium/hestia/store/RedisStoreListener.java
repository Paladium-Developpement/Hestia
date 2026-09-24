package fr.paladium.hestia.store;

import lombok.NonNull;

public interface RedisStoreListener<T> {

	public default void onPostLoad(final @NonNull T object) {}

	public default void onPostDelete(final @NonNull T object) {}

	public default boolean onPreDelete(final @NonNull T object) {
		return false;
	}

	public default boolean onPreSave(final @NonNull T object, final boolean create) {
		return false;
	}

	public default void onPostSave(final @NonNull T object, final String json, final boolean create) {}

}