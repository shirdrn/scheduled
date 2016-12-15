package cn.shiyanjun.platform.scheduled.api;

import java.util.Map;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.api.LifecycleAware;

public interface RecoveryManager extends LifecycleAware {

	Map<Integer, JSONObject> getPendingTaskResponses();
	
}
