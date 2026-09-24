package fr.paladium.hestia.redis.query;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import fr.paladium.hestia.model.TestAccount;
import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.impl.RedisProtocol;
import fr.paladium.hestia.redis.index.RedisIndexResolver;

public class RedisQueryTest {

	@BeforeAll
	public static void register() {
		RedisIndexResolver.resolve(TestAccount.class);
	}

	@Test
	public void rangesAreInclusive() {
		assertEquals(Collections.singletonList("@balance:[1 5]"), RedisQuery.single(TestAccount.class).range("balance", 1L, 5L).getPredicates());
	}

	@Test
	public void tagValuesAreEscaped() {
		assertEquals(Collections.singletonList("@owner:{a\\-b\\ c\\.d}"), RedisQuery.single(TestAccount.class).tag("owner", "a-b c.d").getPredicates());
	}

	@Test
	public void gettersResolveTheirAlias() {
		assertEquals(Collections.singletonList("@owner:{alice}"), RedisQuery.single(TestAccount.class).tag(TestAccount::getOwner, "alice").getPredicates());
	}

	@Test
	public void emptyQueriesMatchEverything() {
		assertArrayEquals(new String[] { "idx:testaccount", "*" }, RedisQuery.list(TestAccount.class).toCommand(new Gson()).getArgs());
	}

	@Test
	public void unindexedGettersAreRejected() {
		assertThrows(IllegalArgumentException.class, () -> RedisQuery.single(TestAccount.class).tag(TestAccount::getId, "id"));
	}

	@Test
	public void cacheKeysDescribeTheWholeQuery() {
		final RedisQuery<List<TestAccount>> query = RedisQuery.list(TestAccount.class).tag("owner", "x").sort("balance", false).limit(0, 10);
		assertEquals("search:idx:testaccount:@owner:{x}:sort=balancedesc:limit=0-10", query.cacheKey());
	}

	@Test
	public void sortedQueriesBecomeSortedSearches() {
		final RedisCommand command = RedisQuery.list(TestAccount.class).tag("owner", "x").sort("balance", true).limit(5, 20).toCommand(new Gson());
		assertEquals(RedisProtocol.FT_SEARCH, command.getCommand());
		assertArrayEquals(new String[] { "idx:testaccount", "@owner:{x}", "SORTBY", "balance", "ASC", "LIMIT", "5", "20" }, command.getArgs());
	}

}