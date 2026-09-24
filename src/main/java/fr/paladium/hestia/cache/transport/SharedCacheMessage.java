package fr.paladium.hestia.cache.transport;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public final class SharedCacheMessage {

	private final String id;
	private final String json;
	private final long version;
	private final String origin;

}