/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.service.impl;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.service.ConfManager;
import org.apache.nutch.service.JobManager;
import org.apache.nutch.service.model.request.JobConfig;
import org.apache.nutch.service.model.response.JobInfo;
import org.apache.nutch.service.model.response.JobInfo.State;
import org.apache.nutch.util.NutchTool;

public class JobManagerImpl implements JobManager {

	private JobFactory jobFactory;
	//	private NutchServerPoolExecutor executor;
	private ConfManager configManager;

	public JobManagerImpl(JobFactory jobFactory, ConfManager configManager) {
		this.jobFactory = jobFactory;
		this.configManager = configManager;		
	}

	@Override
	public Collection<JobInfo> list(String crawlId, State state) {
		
		return null;
	}

	@Override
	public JobInfo get(String crawlId, String id) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * This creates the Job and adds to the ThreadPoolExector
	 */
	@Override
	public String create(JobConfig jobConfig) {
		if (jobConfig.getArgs() == null) {
			throw new IllegalArgumentException("Arguments cannot be null!");
		}
		if(jobConfig.getArgs().size()<2){
			throw new IllegalArgumentException("Required arguments - crawldb, url_dir");
		}
		Configuration conf = cloneConfiguration(jobConfig.getConfId());
		NutchTool tool = createTool(jobConfig, conf);
	    JobWorker worker = new JobWorker(jobConfig, conf, tool);
	    worker.run();		
		return worker.getInfo().getId();
	}
	
	private Configuration cloneConfiguration(String confId) {
		Configuration conf = configManager.get(confId);
		if (conf == null) {
			throw new IllegalArgumentException("Unknown confId " + confId);
		}
		return new Configuration(conf);
	}

	@Override
	public boolean abort(String crawlId, String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean stop(String crawlId, String id) {
		// TODO Auto-generated method stub
		return false;
	}
	
	private NutchTool createTool(JobConfig jobConfig, Configuration conf){
		if(StringUtils.isNotBlank(jobConfig.getJobClassName())){
			return jobFactory.createToolByClassName(jobConfig.getJobClassName(), conf);
		}
		return jobFactory.createToolByType(jobConfig.getType(), conf);
	}
}
