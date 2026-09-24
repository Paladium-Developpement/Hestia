package fr.paladium.hestia.redis;

import com.google.gson.Gson;

import fr.paladium.hestia.redis.json.RedisJsonTypeResolver;
import fr.paladium.hestia.redis.metric.RedisMetricConfig;
import lombok.Getter;
import lombok.NonNull;

@Getter
@SuppressWarnings("unchecked")
public class RedisConfig {

	private final String host;

	private int database;
	private int port = 6379;
	private String password;
	private int poolSize = 16;
	private Gson gson = new Gson();
	private RedisMetricConfig metrics;
	private RedisJsonTypeResolver typeResolver = RedisJsonTypeResolver.IDENTITY;

	protected RedisConfig(final @NonNull String host) {
		this.host = host;
	}

	public static @NonNull RedisConfig create(final @NonNull String host) {
		return new RedisConfig(host);
	}

	public final @NonNull <T extends RedisConfig> T port(final int port) {
		this.port = port;
		return (T) this;
	}

	public final @NonNull <T extends RedisConfig> T database(final int database) {
		this.database = database;
		return (T) this;
	}

	public final @NonNull <T extends RedisConfig> T poolSize(final int poolSize) {
		this.poolSize = poolSize;
		return (T) this;
	}

	public final @NonNull <T extends RedisConfig> T gson(final @NonNull Gson gson) {
		this.gson = gson;
		return (T) this;
	}

	public final @NonNull <T extends RedisConfig> T password(final String password) {
		this.password = password;
		return (T) this;
	}

	public final @NonNull <T extends RedisConfig> T metrics(final RedisMetricConfig metrics) {
		this.metrics = metrics;
		return (T) this;
	}

	public final @NonNull <T extends RedisConfig> T typeResolver(final @NonNull RedisJsonTypeResolver typeResolver) {
		this.typeResolver = typeResolver;
		return (T) this;
	}

}