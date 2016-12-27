package cn.shiyanjun.platform.scheduled.common;

import java.util.Map;

import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import cn.shiyanjun.platform.api.common.AbstractComponent;
import cn.shiyanjun.platform.api.utils.ReflectionUtils;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.RestServer;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;

public abstract class AbstractRestServer extends AbstractComponent implements RestServer {

	private static final Log LOG = LogFactory.getLog(AbstractRestServer.class);
	private Map<String, Class<? extends HttpServlet>> servlets = Maps.newHashMap();
	private final Server server;
	private final ServletContextHandler servletContext;
	private final ComponentManager manager;
	
	public AbstractRestServer(ComponentManager manager) {
		super(manager.getContext());
		this.manager = manager;
		int webPort = context.getInt(ConfigKeys.SCHEDULED_WEB_MANAGER_PORT, 8030);
		server = new Server(webPort);
		servletContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
	}
	
	@Override
	public void start() {
		try {
			servletContext.setContextPath("/");
			server.setHandler(servletContext);
			
			// configure servlets
			servlets.keySet().forEach(path -> {
				Class<? extends HttpServlet> servletClazz = servlets.get(path);
				HttpServlet servlet = ReflectionUtils.newInstance(
						servletClazz, HttpServlet.class, new Object[]{ manager });
				LOG.info("Servlet instance created: path=" + path + ", servlet=" + servlet);
				
				servletContext.addServlet(new ServletHolder(servlet), path);
			});
			
			server.start();
		} catch (Exception e) {
			Throwables.propagate(e);
		} finally {
			stop();
		}
	}

	@Override
	public void stop() {
		try {
			server.stop();
		} catch (Exception e) {
			LOG.warn("Failed to stop HTTP server: ", e);
		}		
	}
	
	@Override
	public void register(String path, Class<? extends HttpServlet> servletClazz) {
		Preconditions.checkArgument(path != null);
		Preconditions.checkArgument(servletClazz != null);
		if(!servlets.containsKey(path)) {
			servlets.put(path, servletClazz);
			LOG.info("Servlet registered: path=" + path + ", servletClazz=" + servletClazz.getName());
		} else {
			LOG.warn("Servlet mapping path already registered: path=" + path);
		}
	}

}
