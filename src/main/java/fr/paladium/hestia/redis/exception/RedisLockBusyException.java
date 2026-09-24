package fr.paladium.hestia.redis.exception;

import lombok.NonNull;

public class RedisLockBusyException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RedisLockBusyException(final @NonNull String key) {
		super("Lock '" + key + "' is already held by another operation.");
	}

}