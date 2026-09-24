package fr.paladium.hestia.redis.exception;

import lombok.NonNull;

public class RedisConnectionException extends Exception {

	private static final long serialVersionUID = 1L;

	public RedisConnectionException(final @NonNull String message, final @NonNull Throwable cause) {
		super(message, cause);
	}

}