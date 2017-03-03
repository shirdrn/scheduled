package cn.shiyanjun.platform.scheduled.api;

import java.util.Optional;

public interface ProtocolManager<P> {

	void initialize();
	
	Optional<P> select(Enum<?> protocolType);
	
	Enum<?> ofType(String protocolType);
}
