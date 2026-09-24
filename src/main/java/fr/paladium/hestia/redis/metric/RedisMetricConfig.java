package fr.paladium.hestia.redis.metric;

import java.time.Duration;

import lombok.Getter;
import lombok.NonNull;

@Getter
@SuppressWarnings("unchecked")
public class RedisMetricConfig {

	private final String prefix;

	private Duration retention = Duration.ofDays(7L);
	private Duration flushInterval = Duration.ofSeconds(10L);

	protected RedisMetricConfig(final @NonNull String prefix) {
		this.prefix = prefix;
	}

	public static @NonNull RedisMetricConfig create(final @NonNull String prefix) {
		return new RedisMetricConfig(prefix);
	}

	public final @NonNull <T extends RedisMetricConfig> T retention(final @NonNull Duration retention) {
		this.retention = retention;
		return (T) this;
	}

	public final @NonNull <T extends RedisMetricConfig> T flushInterval(final @NonNull Duration interval) {
		this.flushInterval = interval;
		return (T) this;
	}

}