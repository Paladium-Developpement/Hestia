package fr.paladium.hestia.redis.lock;

import lombok.NonNull;

public class RedisLockLostException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RedisLockLostException(final @NonNull String key) {
		super("Lock '" + key + "' is no longer held, the write has been rejected.");
	}

}