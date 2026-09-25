package fr.paladium.hestia.bench;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.JsonElement;

import fr.paladium.hestia.redis.json.annotation.RedisJsonSnapshot;
import fr.paladium.hestia.redis.json.annotation.RedisJsonVersion;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BenchDocument {

	private final String id;

	private long counter;
	@RedisJsonVersion private long version;
	private Map<String, BenchEntry> entries = new LinkedHashMap<>();

	@RedisJsonSnapshot
	private transient JsonElement snapshot;

	public BenchDocument(final String id) {
		this.id = id;
	}

	public static BenchDocument create(final String id, final int entries) {
		final BenchDocument document = new BenchDocument(id);
		for (int i = 0; i < entries; i++) {
			document.entries.put("entry-" + i, new BenchEntry(i * 31L, "Entry number " + i + " of " + id, Arrays.asList("alpha", "beta", "gamma", "delta")));
		}
		return document;
	}

}