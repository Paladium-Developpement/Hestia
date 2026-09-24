package fr.paladium.hestia.cache;

import lombok.NonNull;

public interface SharedCacheListener<T> {

	public default void onPostRemove(final @NonNull T removed) {}

	public default void onPostUpdate(final T previous, final @NonNull T current) {}

}