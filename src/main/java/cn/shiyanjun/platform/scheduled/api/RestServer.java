package cn.shiyanjun.platform.scheduled.api;

import javax.servlet.http.HttpServlet;

import cn.shiyanjun.platform.api.LifecycleAware;

public interface RestServer extends LifecycleAware {

	void register(String path, Class<? extends HttpServlet> servletClazz);
}
