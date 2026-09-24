package fr.paladium.hestia.model;

import fr.paladium.hestia.redis.json.utils.RedisJsonOverwrite;
import lombok.Getter;

@Getter
public class TestSquare extends TestShape {

	@RedisJsonOverwrite private long side;

}