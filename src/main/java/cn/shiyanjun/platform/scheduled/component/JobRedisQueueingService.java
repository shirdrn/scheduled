package cn.shiyanjun.platform.scheduled.component;

import java.util.Optional;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.scheduled.common.JobQueueingService;
import cn.shiyanjun.platform.scheduled.constants.ConfigKeys;
import cn.shiyanjun.platform.scheduled.constants.ScheduledConstants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

public class JobRedisQueueingService implements JobQueueingService {

	private final String queue;
	private final JedisPool jedisPool;
	private final int redisDBIndex;
	
    public JobRedisQueueingService(Context context, String queue, JedisPool jedisPool) {
		super();
		Preconditions.checkNotNull(queue);
		this.queue = queue;
		this.jedisPool = jedisPool;
		redisDBIndex = context.getInt(ConfigKeys.SCHEDULED_QUEUEING_REDIS_DB_INDEX, 1);
	}

    @Override
    public synchronized void enqueue(int jobId, String jsonJobDetail) {
    	try (Jedis jedis = jedisPool.getResource()) {
    		jedis.select(redisDBIndex);
    		double score = generateScore();
    		jedis.zadd(queue, score, jsonJobDetail);
    	}
    }
    
    private double generateScore() {
        double score = System.nanoTime();
        try {
			Thread.sleep(1);
		} catch (InterruptedException e) {}
        return score;
    }
    
    @Override
    public synchronized void prioritize(int jobId) {
    	try (Jedis jedis = jedisPool.getResource()) {
    		jedis.select(redisDBIndex);
    		if (jedis.zcard(queue) > 0) {
    			double minScore = -1;
    			String selectedMember = null;
    			Set<Tuple> tuples = jedis.zrangeByScoreWithScores(queue, 0, Double.MAX_VALUE);
    			for(Tuple data : tuples) {
    				JSONObject detail = JSONObject.parseObject(data.getElement());
    				int id = detail.getIntValue(ScheduledConstants.JOB_ID);
    				// retrieve minimum score
    				if(minScore < 0) {
    					minScore = data.getScore();
    				}
    				
    				if(id == jobId) {
    					selectedMember = data.getElement();
    					break;
    				}
    			}
    			Transaction tx = jedis.multi();
    			tx.zrem(queue, selectedMember);
    			double newScore = minScore - 1;
    			tx.zadd(queue, newScore, selectedMember);
    			tx.exec();
    		}
    	}
    }
    
    @Override
	public JSONObject retrieve(int jobId) {
    	JSONObject jobInfo = null;
    	try (Jedis jedis = jedisPool.getResource()) {
    		jedis.select(redisDBIndex);
    		if (jedis.zcard(queue) > 0) {
    			Set<String> records = jedis.zrange(queue, 0, -1);
    			for(String record : records) {
    				JSONObject job = JSONObject.parseObject(record);
    				if(job.containsKey(ScheduledConstants.JOB_ID)) {
    					int queueingJobId = Integer.parseInt(job.get(ScheduledConstants.JOB_ID).toString());
    					if(jobId == queueingJobId) {
    						jobInfo = job;
    						break;
    					}
    				}
    			}
    		}
    	}
		return jobInfo;
	}
    
    @Override
    public synchronized void updateQueuedJob(int jobId, JSONObject newJob) {
    	try (Jedis jedis = jedisPool.getResource()) {
    		jedis.select(redisDBIndex);
    		if (jedis.zcard(queue) > 0) {
    			Optional<Double> score = null;
    			Set<Tuple> tuples = jedis.zrangeByScoreWithScores(queue, 0, Double.MAX_VALUE);
    			String old = null;
    			for(Tuple data : tuples) {
    				JSONObject detail = JSONObject.parseObject(data.getElement());
    				int id = detail.getIntValue(ScheduledConstants.JOB_ID);
    				if(id == jobId) {
    					score = Optional.ofNullable(data.getScore());
    					old = data.getElement();
    					break;
    				}
    			}
    			if(score.isPresent()){
    				Transaction tx = jedis.multi();
    				tx.zrem(queue, old);
    				tx.zadd(queue, score.get(), newJob.toJSONString());
    				tx.exec();
    			}
    		}
    	}
	}
    
    @Override
    public synchronized void remove(String jobId) {
    	try (Jedis jedis = jedisPool.getResource()) {
    		jedis.select(redisDBIndex);
    		for (String json : jedis.zrange(queue, 0, -1)) {
    			JSONObject job = JSONObject.parseObject(json);
    			int queuedJobId = job.getInteger(ScheduledConstants.JOB_ID);
    			if (jobId.equals(String.valueOf(queuedJobId))) {
    				jedis.zrem(queue, json);
    				break;
    			}
    		}
    	}
    }

    @Override
    public Set<String> getJobs() {
    	try (Jedis jedis = jedisPool.getResource()) {
    		jedis.select(redisDBIndex);
    		return jedis.zrange(queue, 0, -1);
    	}
	}

	@Override
	public Set<String> getWaitingJobsBefore(String jobId) {
		Set<String> waitingJobs = Sets.newHashSet();
		try (Jedis jedis = jedisPool.getResource()) {
			jedis.select(redisDBIndex);
			for (String json : jedis.zrange(queue, 0, -1)) {
				JSONObject job = JSONObject.parseObject(json);
				if (jobId.equals(job.getString(ScheduledConstants.JOB_ID))) {
					break;
				} else {
					waitingJobs.add(json);
				}
			}
			return waitingJobs;
		}
	}

	@Override
	public String getQueueName() {
		return queue;
	}

}
