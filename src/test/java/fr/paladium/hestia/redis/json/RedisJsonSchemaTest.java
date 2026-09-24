package fr.paladium.hestia.redis.json;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import fr.paladium.hestia.model.TestAccount;
import fr.paladium.hestia.model.TestHolder;
import fr.paladium.hestia.model.TestShape;
import fr.paladium.hestia.model.TestSquare;

public class RedisJsonSchemaTest {

	private static final JsonObject EMPTY = new JsonObject();

	@Test
	public void unknownFieldsAreDeltas() {
		assertFalse(RedisJsonSchema.of(TestAccount.class, RedisJsonTypeResolver.IDENTITY).child(RedisJsonSchemaTest.EMPTY, "unknown").isOverwrite());
	}

	@Test
	public void serializedNamesAreResolved() {
		assertTrue(RedisJsonSchema.of(TestHolder.class, RedisJsonTypeResolver.IDENTITY).child(RedisJsonSchemaTest.EMPTY, "renamed").isOverwrite());
	}

	@Test
	public void mapValuesInheritTheOverwrite() {
		final RedisJsonSchema baselines = RedisJsonSchema.of(TestAccount.class, RedisJsonTypeResolver.IDENTITY).child(RedisJsonSchemaTest.EMPTY, "baselines");
		assertTrue(baselines.child(RedisJsonSchemaTest.EMPTY, "kills").isOverwrite());
	}

	@Test
	public void annotatedFieldsAreOverwritten() {
		assertTrue(RedisJsonSchema.of(TestAccount.class, RedisJsonTypeResolver.IDENTITY).child(RedisJsonSchemaTest.EMPTY, "lastSeen").isOverwrite());
	}

	@Test
	public void polymorphicTypesNeedTheResolver() {
		final JsonObject shape = JsonParser.parseString("{\"side\":3}").getAsJsonObject();
		final RedisJsonSchema identity = RedisJsonSchema.of(TestHolder.class, RedisJsonTypeResolver.IDENTITY).child(RedisJsonSchemaTest.EMPTY, "shape");
		final RedisJsonSchema resolved = RedisJsonSchema.of(TestHolder.class, (type, json) -> type == TestShape.class ? TestSquare.class : type).child(RedisJsonSchemaTest.EMPTY, "shape");
		assertFalse(identity.child(shape, "side").isOverwrite());
		assertTrue(resolved.child(shape, "side").isOverwrite());
	}

}