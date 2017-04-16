package cn.shiyanjun.platform.scheduled.api;

import java.util.Optional;

public interface QueueSelector {

	void setResourceManager(ResourceManager resourceManager);
	Optional<String> selectQueue();
}
