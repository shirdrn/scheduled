package cn.shiyanjun.platform.scheduled.api;

import java.util.Map;

import com.alibaba.fastjson.JSONObject;

public interface RecoveryManager {

	void recover() throws Exception;
	Map<Integer, JSONObject> getPendingTaskResponses();
	
}
