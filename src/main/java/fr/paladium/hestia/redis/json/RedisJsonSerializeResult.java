package fr.paladium.hestia.redis.json;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

@Getter
@AllArgsConstructor
public final class RedisJsonSerializeResult {

	private final @NonNull RedisJsonPatch patch;
	private final @NonNull Runnable commitLocal;

}