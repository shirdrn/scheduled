package cn.shiyanjun.platform.scheduled.api;

public interface Protocol<IN, OUT> {

	OUT request(IN in);
}
