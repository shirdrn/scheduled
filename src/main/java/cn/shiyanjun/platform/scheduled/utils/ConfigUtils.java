package cn.shiyanjun.platform.scheduled.utils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.common.ContextImpl;
import cn.shiyanjun.platform.api.utils.Pair;

public class ConfigUtils {

	private static final String DEFAULT_CONFIG = "config.properties";
	private static final Context DEFAULT_CONTEXT;
	static {
		DEFAULT_CONTEXT = new ContextImpl(DEFAULT_CONFIG);
	}
	
	public static Context getDefaultContext() {
		return DEFAULT_CONTEXT;
	}
	
	public static Context getContext(String... configs) {
		return new ContextImpl(configs);
	}
	
	// 1,2,3
	public static int[] stringsToInts(String[] strings) {
		return Arrays.stream(strings)
				.mapToInt(s -> Integer.parseInt(s))
				.toArray();
	}
	
	// 1:1,2:1
	public static List<Pair<Integer, Integer>> parsePairStrings(String strings) {
		 return Arrays.stream(strings.split(",")).map(s -> {
			 String[] kv = s.split(":");
			 return new Pair<Integer, Integer>(Integer.parseInt(kv[0]), Integer.parseInt(kv[1]));
		 }).collect(Collectors.toList());
	}

}
