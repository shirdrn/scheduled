package cn.shiyanjun.platform.scheduled.common;

public interface Protocol<IN, OUT> {

	OUT request(IN in);
}
