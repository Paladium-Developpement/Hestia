package fr.paladium.hestia.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonElement;

import fr.paladium.hestia.redis.index.RedisIndex;
import fr.paladium.hestia.redis.json.annotation.RedisJsonOverwrite;
import fr.paladium.hestia.redis.json.annotation.RedisJsonSnapshot;
import fr.paladium.hestia.redis.json.annotation.RedisJsonTransient;
import fr.paladium.hestia.redis.json.annotation.RedisJsonVersion;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TestAccount {

	private final String id;

	private long balance;
	@RedisIndex private String owner;
	@RedisJsonVersion private long version;
	@RedisJsonOverwrite private long lastSeen;
	private List<String> tags = new ArrayList<>();
	@RedisJsonTransient private String description;
	private Map<String, Long> counters = new HashMap<>();
	@RedisJsonOverwrite private Map<String, Long> baselines = new HashMap<>();

	@RedisJsonSnapshot
	private transient JsonElement snapshot;

	public TestAccount(final String id) {
		this.id = id;
	}

}