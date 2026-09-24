package fr.paladium.hestia.transport;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public final class SharedMessage {

	private final String id;
	private final String json;
	private final long version;
	private final String origin;

}