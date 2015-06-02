/**
 * *****************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations
 * under the License. *****************************************************************************
 */
package org.keedio.flume.metrics;

import java.util.concurrent.TimeUnit;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

/**
 *
 * @author <a href="mailto:lalazaro@keedio.com">Luis Lazaro</a>
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 */
public class SqlSourceCounter extends MonitoredCounterGroup implements SqlSourceCounterMBean {

	private long startProcessTime;
	
	private static final String AVERAGE_THROUGHPUT = "average_throughput";
	private static final String CURRENT_THROUGHPUT = "current_throughput";
	private static final String MAX_THROUGHPUT = "max_throughput";
	private static final String EVENT_COUNT = "events_count";
    
    private static final String[] ATTRIBUTES = {AVERAGE_THROUGHPUT, CURRENT_THROUGHPUT, MAX_THROUGHPUT, EVENT_COUNT};
    
    public SqlSourceCounter(String name) {
        super(MonitoredCounterGroup.Type.SOURCE, name, ATTRIBUTES);
    }

    @Override
    public void incrementEventCount(int value) {
        addAndGet(EVENT_COUNT, value);
    }

    @Override
    public long getEventCount() {
        return get(EVENT_COUNT);
    }

    @Override
    public long getAverageThroughput() {
        return get(AVERAGE_THROUGHPUT);
    }
    
    @Override
    public long getCurrentThroughput() {
        return get(CURRENT_THROUGHPUT);
    }
    
    @Override
    public long getMaxThroughput() {
        return get(MAX_THROUGHPUT);
    }
    
    
    public void startProcess(){
    	startProcessTime = System.currentTimeMillis();
    }
    
    public void endProcess(int events){
    	
    	long runningTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - getStartTime());
    	long processTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startProcessTime);
    	long throughput = 0L;
    	
    	if (events > 0 && processTime > 0)
    		throughput = events/processTime;
    		if (getMaxThroughput() < throughput)
    			set(MAX_THROUGHPUT,throughput);
    	
    	if (runningTime > 0 && getEventCount() > 0)
    		set(AVERAGE_THROUGHPUT, (getEventCount()/runningTime));
    	
    	set(CURRENT_THROUGHPUT,throughput);
    }
}
