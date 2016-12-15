package cn.shiyanjun.platform.scheduled.protocols;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.scheduled.api.Protocol;
import cn.shiyanjun.platform.scheduled.common.AbstractProtocolManager;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.protocols.JobOrchestrationProtocolManager.RESTRequest;

public final class JobOrchestrationProtocolManager extends AbstractProtocolManager<Protocol<RESTRequest, JSONObject>> {

	private static final Log LOG = LogFactory.getLog(JobOrchestrationProtocolManager.class);
	public JobOrchestrationProtocolManager(Context context) {
		super(context);
	}
	
	@Override
	public void initialize() {
		register(ProtocolType.REST, new RESTJobOrchestrater());
	}

	private class RESTJobOrchestrater implements Protocol<RESTRequest, JSONObject> {

		private final String jobOrchestrateServiceUrl;
		private final OkHttpClient client = new OkHttpClient();
		private final MediaType mediaType = MediaType.parse("application/json");
		
		public RESTJobOrchestrater() {
			jobOrchestrateServiceUrl = context.get(ConfigKeys.SERVICE_JOB_ORCHESTRATE_URL);
			Preconditions.checkArgument(jobOrchestrateServiceUrl != null, "Job Orchestrate Service url shouldn't be null!");
			LOG.info("Configs: jobOrchestrateServiceUrl=" + jobOrchestrateServiceUrl);
		}
		
		@Override
		public JSONObject request(RESTRequest in) {
			JSONObject jobData = new JSONObject();
			// build a job from known JSON parameters
			Request request = buildHttpRequest(in.jsonParams, in.jobType, in.jobId);
			LOG.info("Prepare to request job orchestrate service: " + request);
			try {
				Response response = client.newCall(request).execute();
				if(response != null && response.isSuccessful()) {
					String result = response.body().string();
					LOG.info("Responsed orchestrated job: " + result);
					jobData = JSONObject.parseObject(result);
					// release all system resources
					response.body().close();
				} else {
					LOG.warn("Fail to parse & orchestrate job: jobId=" + in.jobId + ", jobType=" + in.jobType + ", params=" + in.jsonParams);
				}
			} catch (IOException e) {
				LOG.warn("Failed to orchestrate job: ", e);
			}
			return jobData;
		}
		
		private Request buildHttpRequest(String jobParameters, int jobType, int jobId) {
			String url = jobOrchestrateServiceUrl + "/" + jobType + "/" + jobId;
			RequestBody body = RequestBody.create(mediaType, jobParameters);
			Request request = new Request.Builder()
					.url(url)
					.post(body)
					.addHeader("content-type", "application/json")
					.addHeader("cache-control", "no-cache")
					.build();
			return request;
		}
	}
	
	
	public static class RESTRequest {
		
		String jsonParams;
		int jobId;
		int jobType;
		
		public RESTRequest(String jsonParams, int jobId, int jobType) {
			super();
			this.jsonParams = jsonParams;
			this.jobId = jobId;
			this.jobType = jobType;
		}
	}
	
	@Override
	public Enum<?> ofType(String protocolType) {
		return ProtocolType.valueOf(protocolType);
	}
	
	public enum ProtocolType {
		REST
	}
	
}
