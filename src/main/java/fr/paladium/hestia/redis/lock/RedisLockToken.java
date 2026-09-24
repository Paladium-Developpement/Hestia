package fr.paladium.hestia.redis.lock;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public final class RedisLockToken {

	private final String key;
	private final String token;

}