package fr.paladium.hestia.model;

import com.google.gson.annotations.SerializedName;

import fr.paladium.hestia.redis.json.utils.RedisJsonOverwrite;
import lombok.Getter;

@Getter
public class TestHolder {

	private TestShape shape;
	@RedisJsonOverwrite @SerializedName("renamed") private long original;

}