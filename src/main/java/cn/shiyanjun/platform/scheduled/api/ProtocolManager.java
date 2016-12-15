package cn.shiyanjun.platform.scheduled.api;

public interface ProtocolManager<P> {

	void initialize();
	
	P select(Enum<?> protocolType);
	
	Enum<?> ofType(String protocolType);
}
