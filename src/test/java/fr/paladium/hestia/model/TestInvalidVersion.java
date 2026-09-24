package fr.paladium.hestia.model;

import fr.paladium.hestia.redis.json.annotation.RedisJsonVersion;
import lombok.Getter;

@Getter
public class TestInvalidVersion {

	@RedisJsonVersion private int version;

}