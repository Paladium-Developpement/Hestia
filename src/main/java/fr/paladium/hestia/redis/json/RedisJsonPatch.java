package fr.paladium.hestia.redis.json;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import fr.paladium.hestia.redis.impl.RedisCommand;
import fr.paladium.hestia.redis.lock.RedisLockToken;
import lombok.Getter;
import lombok.NonNull;

@Getter
public final class RedisJsonPatch {

	public static final long APPLIED = 1L;
	public static final long REPLAYED = 0L;
	public static final long REJECTED = -1L;

	private static final String MARKER_SUFFIX = "-patch:";
	private static final String MARKER_TTL_SECONDS = "600";

	private static final String SCRIPT = String.join("\n",
		"local doc, marker, guard = KEYS[1], KEYS[2], KEYS[3]",
		"if redis.call('EXISTS', marker) == 1 then",
		"	return {0, redis.call('JSON.GET', doc)}",
		"end",
		"if guard and redis.call('GET', guard) ~= ARGV[2] then",
		"	return {-1}",
		"end",
		"local function kind(path)",
		"	local result = redis.call('JSON.TYPE', doc, path)",
		"	if type(result) == 'table' then",
		"		return result[1]",
		"	end",
		"	return result",
		"end",
		"local exists = redis.call('EXISTS', doc) == 1",
		"local plan = {}",
		"local index = 4",
		"for _ = 1, tonumber(ARGV[3]) do",
		"	local op, path, parent, first, second = ARGV[index], ARGV[index + 1], ARGV[index + 2], ARGV[index + 3], ARGV[index + 4]",
		"	index = index + 5",
		"	if op == 'S' then",
		"		if path == '$' or (exists and kind(parent)) then",
		"			plan[#plan + 1] = {'S', path, first}",
		"		end",
		"	elseif op == 'D' then",
		"		if exists then",
		"			plan[#plan + 1] = {'D', path}",
		"		end",
		"	elseif op == 'I' then",
		"		if exists then",
		"			local current = kind(path)",
		"			if current == 'integer' or current == 'number' then",
		"				plan[#plan + 1] = {'I', path, first}",
		"			elseif kind(parent) then",
		"				plan[#plan + 1] = {'S', path, second}",
		"			end",
		"		end",
		"	elseif op == 'A' then",
		"		if exists then",
		"			if kind(path) == 'array' then",
		"				plan[#plan + 1] = {'A', path, first}",
		"			elseif kind(parent) then",
		"				plan[#plan + 1] = {'S', path, second}",
		"			end",
		"		end",
		"	elseif op == 'R' then",
		"		if exists and kind(path) == 'array' then",
		"			plan[#plan + 1] = {'R', path, first}",
		"		end",
		"	end",
		"end",
		"redis.call('SET', marker, '1', 'EX', ARGV[1])",
		"for _, step in ipairs(plan) do",
		"	if step[1] == 'S' then",
		"		redis.call('JSON.SET', doc, step[2], step[3])",
		"	elseif step[1] == 'D' then",
		"		redis.call('JSON.DEL', doc, step[2])",
		"	elseif step[1] == 'I' then",
		"		redis.call('JSON.NUMINCRBY', doc, step[2], step[3])",
		"	elseif step[1] == 'A' then",
		"		redis.call('JSON.ARRAPPEND', doc, step[2], step[3])",
		"	elseif step[1] == 'R' then",
		"		local found = redis.call('JSON.ARRINDEX', doc, step[2], step[3])",
		"		local position = type(found) == 'table' and found[1] or found",
		"		if type(position) == 'number' and position >= 0 then",
		"			redis.call('JSON.ARRPOP', doc, step[2], position)",
		"		end",
		"	end",
		"end",
		"return {1, redis.call('JSON.GET', doc)}"
	);

	private final Gson gson;
	private final String key;
	private final String id = UUID.randomUUID().toString();
	private final List<String> operations = new ArrayList<>();

	private RedisLockToken guard;

	private RedisJsonPatch(final @NonNull String key, final @NonNull Gson gson) {
		this.key = key;
		this.gson = gson;
	}

	public static @NonNull RedisJsonPatch create(final @NonNull String key, final @NonNull Gson gson) {
		return new RedisJsonPatch(key, gson);
	}

	public int size() {
		return this.operations.size() / 5;
	}

	public @NonNull String getMarker() {
		final int separator = this.key.indexOf(':');
		return (separator < 0 ? this.key : this.key.substring(0, separator)) + RedisJsonPatch.MARKER_SUFFIX + this.id;
	}

	public @NonNull RedisCommand toCommand() {
		final List<String> arguments = new ArrayList<>(this.operations.size() + 6);
		arguments.add(this.key);
		arguments.add(this.getMarker());
		if (this.guard != null) {
			arguments.add(this.guard.getKey());
		}

		arguments.add(RedisJsonPatch.MARKER_TTL_SECONDS);
		arguments.add(this.guard == null ? "" : this.guard.getToken());
		arguments.add(String.valueOf(this.size()));
		arguments.addAll(this.operations);
		return RedisCommand.Base.eval(RedisJsonPatch.SCRIPT, this.guard == null ? 2 : 3, arguments.toArray(new String[0]));
	}

	public @NonNull RedisJsonPatch delete(final @NonNull String path) {
		return this.add("D", path, "", "", "");
	}

	public @NonNull RedisJsonPatch guard(final @NonNull RedisLockToken token) {
		this.guard = token;
		return this;
	}

	public @NonNull RedisJsonPatch remove(final @NonNull String path, final @NonNull JsonElement element) {
		return this.add("R", path, "", this.gson.toJson(element), "");
	}

	public @NonNull RedisJsonPatch set(final @NonNull String path, final @NonNull String parent, final @NonNull JsonElement value) {
		return this.add("S", path, parent, this.gson.toJson(value), "");
	}

	public @NonNull RedisJsonPatch increment(final @NonNull String path, final @NonNull String parent, final @NonNull String delta, final @NonNull JsonElement target) {
		return this.add("I", path, parent, delta, this.gson.toJson(target));
	}

	public @NonNull RedisJsonPatch append(final @NonNull String path, final @NonNull String parent, final @NonNull JsonElement element, final @NonNull JsonElement target) {
		return this.add("A", path, parent, this.gson.toJson(element), this.gson.toJson(target));
	}

	private @NonNull RedisJsonPatch add(final @NonNull String op, final @NonNull String path, final @NonNull String parent, final @NonNull String first, final @NonNull String second) {
		this.operations.add(op);
		this.operations.add(path);
		this.operations.add(parent);
		this.operations.add(first);
		this.operations.add(second);
		return this;
	}

}