package fr.paladium.hestia.model;

import java.util.UUID;

import fr.paladium.hestia.redis.index.RedisIndex;
import lombok.Getter;

@Getter
public class TestMember {

	@RedisIndex private UUID uuid;

}