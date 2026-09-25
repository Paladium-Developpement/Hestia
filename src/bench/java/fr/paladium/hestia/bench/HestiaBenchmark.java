package fr.paladium.hestia.bench;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.google.gson.Gson;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.RedisConfig;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.impl.RedisProtocol;
import fr.paladium.hestia.redis.json.RedisJsonPatch;
import fr.paladium.hestia.store.RedisStore;
import fr.paladium.hestia.store.RedisStoreConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public final class HestiaBenchmark {

	private static final int THREADS = 8;
	private static final int WRITERS = 16;
	private static final int READS = 2000;
	private static final int WARMUP = 300;
	private static final int ENTRIES = 150;
	private static final int INCREMENTS = 250;
	private static final int ITERATIONS = 2000;
	private static final Gson GSON = new Gson();
	private static final int LARGE_ENTRIES = 1200;
	private static final String IMAGE = "redis:8.6.2";

	private HestiaBenchmark() {}

	public static void main(final String[] args) throws Exception {
		if (args.length == 2) {
			HestiaBenchmark.run(args[0], Integer.parseInt(args[1]));
			return;
		}

		try (GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse(HestiaBenchmark.IMAGE)).withExposedPorts(6379)) {
			redis.start();
			HestiaBenchmark.run(redis.getHost(), redis.getMappedPort(6379));
		}
	}

	private static String size(final long bytes) {
		return bytes < 1024L ? bytes + " o" : String.format(Locale.ROOT, "%.1f Ko", bytes / 1024D);
	}

	private static String micros(final long nanos) {
		return String.format(Locale.ROOT, "%.2f ms", nanos / 1_000_000D);
	}

	private static String millis(final long nanos) {
		return String.format(Locale.ROOT, "%.0f ms", nanos / 1_000_000D);
	}

	private static long bytes(final String... args) {
		long total = 0L;
		for (final String arg : args) {
			total += arg.getBytes(StandardCharsets.UTF_8).length;
		}
		return total;
	}

	private static long[] measure(final Runnable operation) {
		for (int i = 0; i < HestiaBenchmark.WARMUP; i++) {
			operation.run();
		}

		final long[] times = new long[HestiaBenchmark.ITERATIONS];
		for (int i = 0; i < HestiaBenchmark.ITERATIONS; i++) {
			final long start = System.nanoTime();
			operation.run();
			times[i] = System.nanoTime() - start;
		}
		Arrays.sort(times);
		return times;
	}

	private static String rate(final long operations, final long nanos) {
		return String.format(Locale.ROOT, "%,.0f ops/s", operations / (nanos / 1_000_000_000D));
	}

	private static String percent(final double cpuMicros, final long nanos) {
		return String.format(Locale.ROOT, "%.0f %%", cpuMicros * 100D / (nanos / 1_000D));
	}

	private static double redisMicros(final Jedis admin, final String command) {
		for (final String line : admin.info("commandstats").split("\r?\n")) {
			if (line.startsWith("cmdstat_" + command + ":")) {
				for (final String field : line.substring(line.indexOf(':') + 1).split(",")) {
					if (field.startsWith("usec_per_call=")) {
						return Double.parseDouble(field.substring("usec_per_call=".length()));
					}
				}
			}
		}
		return 0D;
	}

	private static void run(final String host, final int port) throws Exception {
		try (Jedis admin = new Jedis(host, port); RedisClient client = RedisClient.create(RedisConfig.create(host).port(port)); RedisStore<BenchDocument> store = RedisStore.create(client, RedisStoreConfig.create(BenchDocument.class, BenchDocument::getId))) {
			System.out.println("Java " + System.getProperty("java.version") + ", " + Runtime.getRuntime().availableProcessors() + " coeurs, " + System.getProperty("os.name") + ", Redis " + admin.info("server").replaceAll("(?s).*redis_version:([^\r\n]+).*", "$1"));
			System.out.println();
			HestiaBenchmark.updates(client, store, admin, HestiaBenchmark.ENTRIES);
			HestiaBenchmark.updates(client, store, admin, HestiaBenchmark.LARGE_ENTRIES);
			HestiaBenchmark.load(client, store, admin);
			HestiaBenchmark.contention(client, store, host, port);
			HestiaBenchmark.reads(client, store);
		}
	}

	private static long parallel(final int writers, final Writer increment) throws Exception {
		final ExecutorService pool = Executors.newFixedThreadPool(writers);
		final long start = System.nanoTime();
		final List<Future<?>> tasks = new ArrayList<>();
		for (int thread = 0; thread < writers; thread++) {
			final int writer = thread;
			tasks.add(pool.submit(() -> {
				for (int i = 0; i < HestiaBenchmark.INCREMENTS; i++) {
					increment.run(writer);
				}
			}));
		}

		for (final Future<?> task : tasks) {
			task.get();
		}
		pool.shutdown();
		return System.nanoTime() - start;
	}

	private static String row(final String name, final long bytes, final double cpu, final long[] times) {
		long total = 0L;
		for (final long time : times) {
			total += time;
		}
		return "| " + name + " | " + HestiaBenchmark.size(bytes) + " | " + String.format(Locale.ROOT, "%.0f µs", cpu) + " | " + HestiaBenchmark.micros(times[times.length / 2]) + " | " + HestiaBenchmark.micros(times[(int) (times.length * 0.99D)]) + " | " + HestiaBenchmark.rate(times.length, total) + " |";
	}

	private static void reads(final RedisClient client, final RedisStore<BenchDocument> store) throws Exception {
		final List<String> ids = new ArrayList<>(HestiaBenchmark.READS);
		for (int i = 0; i < HestiaBenchmark.READS; i++) {
			final BenchDocument document = BenchDocument.create("read-" + i, 10);
			client.execute(RedisCommand.Json.set(HestiaBenchmark.GSON, store.getKey(document.getId()), document));
			ids.add(document.getId());
		}

		final ExecutorService pool = Executors.newFixedThreadPool(HestiaBenchmark.THREADS * 2);
		final long naiveStart = System.nanoTime();
		final List<Future<?>> naive = new ArrayList<>();
		for (final String id : ids) {
			naive.add(pool.submit(() -> HestiaBenchmark.GSON.fromJson(client.<String>execute(RedisCommand.Json.get(store.getKey(id))), BenchDocument.class)));
		}
		for (final Future<?> future : naive) {
			future.get();
		}
		final long naiveTime = System.nanoTime() - naiveStart;
		pool.shutdown();

		final long hestiaStart = System.nanoTime();
		final List<CompletableFuture<BenchDocument>> hestia = new ArrayList<>();
		for (final String id : ids) {
			hestia.add(store.fetch(id));
		}
		CompletableFuture.allOf(hestia.toArray(new CompletableFuture[0])).get();
		final long hestiaTime = System.nanoTime() - hestiaStart;

		final long lingerTime;
		try (RedisStore<BenchDocument> lingering = RedisStore.create(client, RedisStoreConfig.create(BenchDocument.class, BenchDocument::getId).readFlushInterval(Duration.ofMillis(50L)))) {
			final long lingerStart = System.nanoTime();
			final List<CompletableFuture<BenchDocument>> lingered = new ArrayList<>();
			for (final String id : ids) {
				lingered.add(lingering.fetch(id));
			}
			CompletableFuture.allOf(lingered.toArray(new CompletableFuture[0])).get();
			lingerTime = System.nanoTime() - lingerStart;
		}

		final long hotStart = System.nanoTime();
		final List<CompletableFuture<BenchDocument>> hot = new ArrayList<>();
		for (int i = 0; i < HestiaBenchmark.READS; i++) {
			hot.add(store.fetch(ids.get(0)));
		}
		CompletableFuture.allOf(hot.toArray(new CompletableFuture[0])).get();
		final long hotTime = System.nanoTime() - hotStart;

		System.out.println("## Lecture de " + HestiaBenchmark.READS + " objets depuis " + HestiaBenchmark.THREADS * 2 + " threads");
		System.out.println();
		System.out.println("| Approche | Duree | Debit |");
		System.out.println("|---|---|---|");
		System.out.println("| Un JSON.GET par lecture (pool de connexions) | " + HestiaBenchmark.millis(naiveTime) + " | " + HestiaBenchmark.rate(HestiaBenchmark.READS, naiveTime) + " |");
		System.out.println("| Hestia (lectures regroupees en pipeline) | " + HestiaBenchmark.millis(hestiaTime) + " | " + HestiaBenchmark.rate(HestiaBenchmark.READS, hestiaTime) + " |");
		System.out.println("| Hestia, readFlushInterval 50 ms | " + HestiaBenchmark.millis(lingerTime) + " | " + HestiaBenchmark.rate(HestiaBenchmark.READS, lingerTime) + " |");
		System.out.println("| Hestia, " + HestiaBenchmark.READS + " lectures du meme objet (dedupliquees) | " + HestiaBenchmark.millis(hotTime) + " | " + HestiaBenchmark.rate(HestiaBenchmark.READS, hotTime) + " |");
		System.out.println();
	}

	private static String seed(final RedisClient client, final RedisStore<BenchDocument> store, final String id) {
		final String key = store.getKey(id);
		client.execute(RedisCommand.Json.set(HestiaBenchmark.GSON, key, BenchDocument.create(id, HestiaBenchmark.ENTRIES)));
		return key;
	}

	private static long parallelWithJedis(final String host, final int port, final JedisIncrement increment) throws Exception {
		final ExecutorService pool = Executors.newFixedThreadPool(HestiaBenchmark.THREADS);
		final long start = System.nanoTime();
		final List<Future<?>> tasks = new ArrayList<>();
		for (int thread = 0; thread < HestiaBenchmark.THREADS; thread++) {
			tasks.add(pool.submit(() -> {
				try (Jedis jedis = new Jedis(host, port)) {
					for (int i = 0; i < HestiaBenchmark.INCREMENTS; i++) {
						increment.run(jedis);
					}
				}
			}));
		}

		for (final Future<?> task : tasks) {
			task.get();
		}
		pool.shutdown();
		return System.nanoTime() - start;
	}

	private static void load(final RedisClient client, final RedisStore<BenchDocument> store, final Jedis admin) throws Exception {
		final int writes = HestiaBenchmark.WRITERS * HestiaBenchmark.INCREMENTS;
		final List<BenchDocument> rewritten = new ArrayList<>();
		final List<BenchDocument> patched = new ArrayList<>();
		for (int i = 0; i < HestiaBenchmark.WRITERS; i++) {
			rewritten.add(BenchDocument.create("load-full-" + i, HestiaBenchmark.LARGE_ENTRIES));
			final BenchDocument document = BenchDocument.create("load-patch-" + i, HestiaBenchmark.LARGE_ENTRIES);
			store.save(document).get();
			patched.add(document);
		}

		admin.configResetStat();
		final long fullTime = HestiaBenchmark.parallel(HestiaBenchmark.WRITERS, writer -> {
			final BenchDocument document = rewritten.get(writer);
			document.setCounter(document.getCounter() + 1L);
			client.execute(RedisCommand.Json.set(HestiaBenchmark.GSON, store.getKey(document.getId()), document));
		});
		final double fullCpu = HestiaBenchmark.redisMicros(admin, "json.set") * writes;

		admin.configResetStat();
		final long patchTime = HestiaBenchmark.parallel(HestiaBenchmark.WRITERS, writer -> {
			final BenchDocument document = patched.get(writer);
			document.setCounter(document.getCounter() + 1L);
			store.save(document).join();
		});
		final double patchCpu = HestiaBenchmark.redisMicros(admin, "evalsha") * writes;

		System.out.println("## " + HestiaBenchmark.WRITERS + " ecrivains sur des objets differents de " + HestiaBenchmark.size(HestiaBenchmark.bytes(HestiaBenchmark.GSON.toJson(rewritten.get(0)))) + " (" + writes + " ecritures)");
		System.out.println();
		System.out.println("| Approche | Duree | Debit | CPU Redis occupe |");
		System.out.println("|---|---|---|---|");
		System.out.println("| JSON.SET du document complet | " + HestiaBenchmark.millis(fullTime) + " | " + HestiaBenchmark.rate(writes, fullTime) + " | " + HestiaBenchmark.percent(fullCpu, fullTime) + " |");
		System.out.println("| Hestia (patch atomique) | " + HestiaBenchmark.millis(patchTime) + " | " + HestiaBenchmark.rate(writes, patchTime) + " | " + HestiaBenchmark.percent(patchCpu, patchTime) + " |");
		System.out.println();
	}

	private static void updates(final RedisClient client, final RedisStore<BenchDocument> store, final Jedis admin, final int entries) throws Exception {
		final BenchDocument patched = BenchDocument.create("update-patch-" + entries, entries);
		store.save(patched).get();
		admin.configResetStat();
		final long[] patchTimes = HestiaBenchmark.measure(() -> {
			patched.setCounter(patched.getCounter() + 1L);
			store.save(patched).join();
		});
		final double patchCpu = HestiaBenchmark.redisMicros(admin, "evalsha");

		final BenchDocument rewritten = BenchDocument.create("update-full-" + entries, entries);
		final String key = store.getKey(rewritten.getId());
		admin.configResetStat();
		final long[] fullTimes = HestiaBenchmark.measure(() -> {
			rewritten.setCounter(rewritten.getCounter() + 1L);
			client.execute(RedisCommand.Json.set(HestiaBenchmark.GSON, key, rewritten));
		});
		final double fullCpu = HestiaBenchmark.redisMicros(admin, "json.set");

		patched.setCounter(patched.getCounter() + 1L);
		final RedisJsonPatch patch = client.getJsonSerializer().prepare(store.getKey(patched.getId()), patched, client.getJsonSerializer().capture(patched)).getPatch();
		final String[] args = patch.toShaCommand().getArgs();
		final long patchBytes = HestiaBenchmark.bytes(args);
		final long fullBytes = HestiaBenchmark.bytes(RedisCommand.Json.set(HestiaBenchmark.GSON, key, rewritten).getArgs());

		System.out.println("## Mise a jour d'un champ dans un document de " + HestiaBenchmark.size(fullBytes) + " (" + HestiaBenchmark.ITERATIONS + " ecritures)");
		System.out.println();
		System.out.println("| Approche | Envoye par ecriture | CPU Redis par ecriture | p50 | p99 | Debit |");
		System.out.println("|---|---|---|---|---|---|");
		System.out.println(HestiaBenchmark.row("JSON.SET du document complet", fullBytes, fullCpu, fullTimes));
		System.out.println(HestiaBenchmark.row("Hestia (patch atomique)", patchBytes, patchCpu, patchTimes));
		System.out.println();
	}

	private static void contention(final RedisClient client, final RedisStore<BenchDocument> store, final String host, final int port) throws Exception {
		final int expected = HestiaBenchmark.THREADS * HestiaBenchmark.INCREMENTS;
		System.out.println("## " + HestiaBenchmark.THREADS + " ecrivains concurrents, " + HestiaBenchmark.INCREMENTS + " increments chacun sur le meme objet (" + expected + " attendus)");
		System.out.println();
		System.out.println("| Approche | Valeur finale | Ecritures perdues | Rejeux | Duree | Debit |");
		System.out.println("|---|---|---|---|---|---|");

		final String naiveKey = HestiaBenchmark.seed(client, store, "contention-naive");
		final long naiveTime = HestiaBenchmark.parallel(HestiaBenchmark.THREADS, writer -> {
			final BenchDocument document = HestiaBenchmark.GSON.fromJson(client.<String>execute(RedisCommand.Json.get(naiveKey)), BenchDocument.class);
			document.setCounter(document.getCounter() + 1L);
			client.execute(RedisCommand.Json.set(HestiaBenchmark.GSON, naiveKey, document));
		});
		System.out.println(HestiaBenchmark.contentionRow("Relire puis JSON.SET", client, naiveKey, expected, 0L, naiveTime));

		final String optimisticKey = HestiaBenchmark.seed(client, store, "contention-optimistic");
		final AtomicLong optimisticRetries = new AtomicLong();
		final long optimisticTime = HestiaBenchmark.parallelWithJedis(host, port, jedis -> {
			while (true) {
				jedis.watch(optimisticKey);
				final String json = new String((byte[]) jedis.sendCommand(RedisProtocol.JSON_GET, optimisticKey), StandardCharsets.UTF_8);
				final BenchDocument document = HestiaBenchmark.GSON.fromJson(json, BenchDocument.class);
				document.setCounter(document.getCounter() + 1L);
				final Transaction transaction = jedis.multi();
				transaction.sendCommand(RedisProtocol.JSON_SET, optimisticKey, RedisCommand.ROOT_PATH, HestiaBenchmark.GSON.toJson(document));
				if (transaction.exec() != null) {
					return;
				}
				optimisticRetries.incrementAndGet();
			}
		});
		System.out.println(HestiaBenchmark.contentionRow("WATCH / MULTI (optimiste)", client, optimisticKey, expected, optimisticRetries.get(), optimisticTime));

		final String hestiaKey = HestiaBenchmark.seed(client, store, "contention-hestia");
		final String hestiaId = hestiaKey.substring(hestiaKey.indexOf(':') + 1);
		final long hestiaTime = HestiaBenchmark.parallel(HestiaBenchmark.THREADS, writer -> {
			final BenchDocument document = store.fetch(hestiaId).join();
			document.setCounter(document.getCounter() + 1L);
			store.save(document).join();
		});
		System.out.println(HestiaBenchmark.contentionRow("Hestia", client, hestiaKey, expected, 0L, hestiaTime));
		System.out.println();
	}

	private static String contentionRow(final String name, final RedisClient client, final String key, final int expected, final long retries, final long time) {
		final long value = HestiaBenchmark.GSON.fromJson(client.<String>execute(RedisCommand.Json.get(key)), BenchDocument.class).getCounter();
		return "| " + name + " | " + value + " | " + (expected - value) + " | " + retries + " | " + HestiaBenchmark.millis(time) + " | " + HestiaBenchmark.rate(expected, time) + " |";
	}

	@FunctionalInterface
	private interface Writer {

		public void run(final int writer);

	}

	@FunctionalInterface
	private interface JedisIncrement {

		public void run(final Jedis jedis);

	}

}