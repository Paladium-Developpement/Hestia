package fr.paladium.hestia.redis.metric.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.impl.RedisProtocol;

public class RedisMetricTest {

	@Test
	public void gaugesOnlyFlushWhenChanged() {
		final RedisMetricAsyncGauge gauge = new RedisMetricAsyncGauge("gauge", 1000L);
		gauge.set(2L);

		final List<RedisCommand> commands = RedisMetricTest.drain(gauge::drain);
		assertEquals(1, commands.size());
		assertEquals("2.0", commands.get(0).getArgs()[2]);
		assertTrue(RedisMetricTest.drain(gauge::drain).isEmpty());
	}

	@Test
	public void countersFlushTheirDeltaOnce() {
		final RedisMetricAsyncCounter counter = new RedisMetricAsyncCounter("counter", 1000L);
		counter.increment(5L);
		counter.increment();

		final List<RedisCommand> commands = RedisMetricTest.drain(counter::drain);
		assertEquals(RedisProtocol.TS_INCRBY, commands.get(0).getCommand());
		assertEquals("6.0", commands.get(0).getArgs()[1]);
		assertTrue(RedisMetricTest.drain(counter::drain).isEmpty());
	}

	@Test
	public void timersFlushCountSumMinAndMax() {
		final RedisMetricAsyncTimer timer = new RedisMetricAsyncTimer("timer", 1000L);
		timer.record(5L);
		timer.record(10L);
		timer.record(3L);

		final List<RedisCommand> commands = RedisMetricTest.drain(timer::drain);
		final String[] keys = new String[commands.size()];
		final String[] values = new String[commands.size()];
		for (int i = 0; i < commands.size(); i++) {
			keys[i] = commands.get(i).getArgs()[0];
			values[i] = commands.get(i).getArgs()[2];
		}

		assertArrayEquals(new String[] { "timer.count", "timer.sum", "timer.min", "timer.max" }, keys);
		assertArrayEquals(new String[] { "3.0", "18.0", "3.0", "10.0" }, values);
		assertTrue(RedisMetricTest.drain(timer::drain).isEmpty());
	}

	@Test
	public void noOpRecorderStillRunsOperations() {
		final AtomicBoolean ran = new AtomicBoolean();
		RedisMetricNoOpRecorder.INSTANCE.timer("timer").record(() -> ran.set(true));
		assertTrue(ran.get());
		assertEquals("value", RedisMetricNoOpRecorder.INSTANCE.timer("timer").record(() -> "value"));
	}

	private static List<RedisCommand> drain(final Consumer<List<RedisCommand>> drain) {
		final List<RedisCommand> commands = new ArrayList<>();
		drain.accept(commands);
		return commands;
	}

}