package cn.shiyanjun.platform.scheduled.common;

import java.util.Map;
import java.util.Optional;

import com.google.common.collect.Maps;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.api.common.AbstractComponent;
import cn.shiyanjun.platform.scheduled.api.ProtocolManager;

public abstract class AbstractProtocolManager<P> extends AbstractComponent implements ProtocolManager<P> {

	private final Map<Enum<?>, P> protocols = Maps.newHashMap();
	
	public AbstractProtocolManager(Context context) {
		super(context);
	}
	
	protected void register(Enum<?> protocolType, P protocol) {
		protocols.put(protocolType, protocol);
	}
	
	@Override
	public Optional<P> select(Enum<?> protocolType) {
		return Optional.ofNullable(protocols.get(protocolType));
	}

}
