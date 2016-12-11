package cn.shiyanjun.platform.scheduled.common;

import java.util.Map;

import com.google.common.collect.Maps;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.common.AbstractComponent;

public abstract class AbstractProtocolManager<P> extends AbstractComponent implements ProtocolManager<P> {

	private final Map<Enum<?>, P> protocols = Maps.newHashMap();
	
	public AbstractProtocolManager(Context context) {
		super(context);
	}
	
	protected void register(Enum<?> protocolType, P protocol) {
		protocols.put(protocolType, protocol);
	}
	
	@Override
	public P select(Enum<?> protocolType) {
		return protocols.get(protocolType);
	}

}
