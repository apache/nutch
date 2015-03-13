package org.apache.nutch.service;

import java.util.Collection;

import org.apache.nutch.service.model.response.JobConfig;
import org.apache.nutch.service.model.response.JobInfo;
import org.apache.nutch.service.model.response.JobInfo.State;

public interface JobManager {
	
	public static enum JobType{
		INJECT, GENERATE, FETCH, PARSE, UPDATEDB, INDEX, READDB, CLASS
	};
	public Collection<JobInfo> list(String crawlId, State state);

	public JobInfo get(String crawlId, String id);

	public String create(JobConfig jobConfig);
	
	public boolean abort(String crawlId, String id);

	public boolean stop(String crawlId, String id);
}
