package fr.paladium.hestia.cache;

import lombok.NonNull;

public interface SharedCacheListener<T> {

	public default void onRemove(final @NonNull T removed) {}

	public default void onUpdate(final T previous, final @NonNull T current) {}

}