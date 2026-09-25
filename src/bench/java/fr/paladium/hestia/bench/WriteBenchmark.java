package fr.paladium.hestia.bench;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import fr.paladium.hestia.redis.RedisClient;
import fr.paladium.hestia.redis.RedisConfig;
import fr.paladium.hestia.store.RedisStore;
import fr.paladium.hestia.store.RedisStoreConfig;

public final class WriteBenchmark {

	private static final int WARMUP = 20;

	private WriteBenchmark() {}

	public static void main(final String[] args) throws Exception {
		final String label = args[2];
		try (RedisClient client = RedisClient.create(RedisConfig.create(args[0]).port(Integer.parseInt(args[1]))); RedisStore<BenchDocument> store = RedisStore.create(client, RedisStoreConfig.create(BenchDocument.class, BenchDocument::getId))) {
			WriteBenchmark.run(store, label, 64, 20, 150);
			WriteBenchmark.run(store, label, 16, 1200, 150);
		}
	}

	private static long parallel(final RedisStore<BenchDocument> store, final List<BenchDocument> documents, final int saves) throws Exception {
		final ExecutorService pool = Executors.newFixedThreadPool(documents.size());
		final long start = System.nanoTime();
		final List<Future<?>> tasks = new ArrayList<>();
		for (final BenchDocument document : documents) {
			tasks.add(pool.submit(() -> {
				for (int i = 0; i < saves; i++) {
					document.setCounter(document.getCounter() + 1L);
					store.save(document).join();
				}
			}));
		}

		for (final Future<?> task : tasks) {
			task.get();
		}
		pool.shutdown();
		return System.nanoTime() - start;
	}

	private static void run(final RedisStore<BenchDocument> store, final String label, final int writers, final int entries, final int saves) throws Exception {
		final List<BenchDocument> documents = new ArrayList<>(writers);
		for (int i = 0; i < writers; i++) {
			final BenchDocument document = BenchDocument.create(label + "-" + writers + "-" + entries + "-" + i, entries);
			store.save(document).get();
			documents.add(document);
		}

		WriteBenchmark.parallel(store, documents, WriteBenchmark.WARMUP);
		final long time = WriteBenchmark.parallel(store, documents, saves);
		final int total = writers * saves;
		System.out.println(String.format(Locale.ROOT, "| %s | %d ecrivains, %d entrees | %d ecritures | %.0f ms | %,.0f ops/s |", label, writers, entries, total, time / 1_000_000D, total / (time / 1_000_000_000D)));
	}

}