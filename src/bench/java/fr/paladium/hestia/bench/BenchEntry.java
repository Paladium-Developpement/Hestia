package fr.paladium.hestia.bench;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class BenchEntry {

	private long score;
	private String name;
	private List<String> tags;

}