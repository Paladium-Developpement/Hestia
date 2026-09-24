package fr.paladium.hestia.redis.json;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import fr.paladium.hestia.model.TestAccount;
import fr.paladium.hestia.redis.json.utils.RedisJsonTransientExclusionStrategy;

public class RedisJsonTransientTest {

	@Test
	public void transientFieldsAreNotWritten() {
		final Gson gson = new GsonBuilder().addSerializationExclusionStrategy(RedisJsonTransientExclusionStrategy.INSTANCE).create();
		final TestAccount account = new TestAccount("transient");
		account.setDescription("derived");
		account.setOwner("alice");

		final JsonObject json = gson.toJsonTree(account).getAsJsonObject();
		assertFalse(json.has("description"));
		assertTrue(json.has("owner"));
		assertTrue(new Gson().toJsonTree(account).getAsJsonObject().has("description"));
	}

}