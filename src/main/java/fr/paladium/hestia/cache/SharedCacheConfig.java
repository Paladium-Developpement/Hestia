package fr.paladium.hestia.cache;

import java.time.Duration;

import fr.paladium.hestia.transport.SharedTransport;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Getter
@SuppressWarnings("unchecked")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class SharedCacheConfig {

	private SharedTransport transport;
	private Duration refreshInterval = Duration.ofMinutes(1L);

	public static @NonNull SharedCacheConfig create() {
		return new SharedCacheConfig();
	}

	public final @NonNull <T extends SharedCacheConfig> T transport(final @NonNull SharedTransport transport) {
		this.transport = transport;
		return (T) this;
	}

	public final @NonNull <T extends SharedCacheConfig> T refreshInterval(final @NonNull Duration refreshInterval) {
		this.refreshInterval = refreshInterval;
		return (T) this;
	}

}