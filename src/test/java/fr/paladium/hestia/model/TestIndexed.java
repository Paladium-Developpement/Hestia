package fr.paladium.hestia.model;

import java.util.List;

import fr.paladium.hestia.redis.index.RedisIndex;
import lombok.Getter;

@Getter
public class TestIndexed {

	private TestMember leader;
	@RedisIndex private String name;
	private List<TestMember> members;
	@RedisIndex(sortable = true) private long score;

}