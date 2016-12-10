package cn.shiyanjun.platform.scheduled.utils;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.common.ContextImpl;

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

}
