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

import org.apache.flume.instrumentation.MonitoredCounterGroup;

/**
 *
 * @author Luis Lázaro <lalazaro@keedio.com>
 */
public class SqlSourceCounter extends MonitoredCounterGroup implements SqlSourceCounterMBean {

    private static long rows_count,
            rows_proc,
            sendThroughput,
            eventCount,
            last_sent,
            start_time;

    private static final String[] ATTRIBUTES = {"rows_count", "rows_proc", "sendThroughput",
        "eventCount", "last_sent", "start_time"};

    public SqlSourceCounter(String name) {
        super(MonitoredCounterGroup.Type.SOURCE, name, ATTRIBUTES);
        rows_count = 0;
        rows_proc = 0;
        sendThroughput = 0;
        eventCount = 0;
        last_sent = 0;
        start_time = System.currentTimeMillis();
    }

    @Override
    public long getRowsCount() {
        return rows_count;
    }

    @Override
    public void incrementRowsCount() {
        rows_count++;
    }

    @Override
    public long getRowsProc() {
        return rows_proc;
    }

    @Override
    public void incrementRowsProc() {
        rows_proc++;
    }

    @Override
    public void incrementEventCount() {
        last_sent = System.currentTimeMillis();
        eventCount++;
        if (last_sent - start_time >= 1000) {
            long secondsElapsed = (last_sent - start_time) / 1000;
            sendThroughput = eventCount / secondsElapsed;
        }
    }

    @Override
    public long getEventCount() {
        return eventCount;
    }

    @Override
    public long getSendThroughput() {
        return sendThroughput;
    }
}
