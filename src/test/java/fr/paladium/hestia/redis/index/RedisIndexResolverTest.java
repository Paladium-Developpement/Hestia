package fr.paladium.hestia.redis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import fr.paladium.hestia.model.TestIndexed;
import fr.paladium.hestia.model.TestMember;
import fr.paladium.hestia.model.TestShape;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.impl.RedisProtocol;

public class RedisIndexResolverTest {

	@Test
	public void namesFollowTheClass() {
		assertEquals("idx:testindexed", RedisIndexResolver.indexName(TestIndexed.class));
		assertEquals("testindexed:", RedisIndexResolver.prefix(TestIndexed.class));
	}

	@Test
	public void aliasesFlattenThePath() {
		assertEquals("leader_uuid", RedisIndexResolver.toAlias("$.leader.uuid"));
		assertEquals("members_uuid", RedisIndexResolver.toAlias("$.members[*].uuid"));
		assertEquals("data_level", RedisIndexResolver.toAlias("$.data.*.level"));
	}

	@Test
	public void unindexedClassesHaveNoIndex() {
		assertNull(RedisIndexResolver.resolve(TestShape.class));
	}

	@Test
	public void aliasesRequireARegisteredIndex() {
		assertThrows(IllegalArgumentException.class, () -> RedisIndexResolver.aliasFor(TestMember.class, TestMember.class, "uuid"));
	}

	@Test
	public void schemaCoversNestedObjectsAndLists() {
		final RedisCommand command = RedisIndexResolver.resolve(TestIndexed.class);
		final List<String> args = Arrays.asList(command.getArgs());
		assertEquals(RedisProtocol.FT_CREATE, command.getCommand());
		assertEquals(Arrays.asList("idx:testindexed", "ON", "JSON", "PREFIX", "1", "testindexed:", "SCHEMA"), args.subList(0, 7));
		assertTrue(RedisIndexResolverTest.contains(args, "$.name", "AS", "name", "TAG", "SEPARATOR", ";"));
		assertTrue(RedisIndexResolverTest.contains(args, "$.score", "AS", "score", "NUMERIC", "SORTABLE"));
		assertTrue(RedisIndexResolverTest.contains(args, "$.leader.uuid", "AS", "leader_uuid", "TAG"));
		assertTrue(RedisIndexResolverTest.contains(args, "$.members[*].uuid", "AS", "members_uuid", "TAG"));
		assertEquals("score", RedisIndexResolver.aliasFor(TestIndexed.class, TestIndexed.class, "score"));
	}

	private static boolean contains(final List<String> args, final String... sequence) {
		return Collections.indexOfSubList(args, Arrays.asList(sequence)) >= 0;
	}

}