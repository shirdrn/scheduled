package cn.shiyanjun.platform.scheduled.common;

public interface ProtocolManager<P> {

	void initialize();
	
	P select(Enum<?> protocolType);
	
	Enum<?> ofType(String protocolType);
}
