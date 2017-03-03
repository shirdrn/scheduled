package cn.shiyanjun.platform.scheduled.utils;

public class HookUtils {

	public static void addShutdownHook(Runnable block) {
		Runtime.getRuntime().addShutdownHook(new Thread(block));
	}
}
