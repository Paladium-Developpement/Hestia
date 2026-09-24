package fr.paladium.hestia.redis.json.utils;

import fr.paladium.hestia.redis.json.RedisJsonPatch;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

@Getter
@AllArgsConstructor
public final class RedisJsonSerializeResult {

	private final @NonNull RedisJsonPatch patch;
	private final @NonNull Runnable commitLocal;

}