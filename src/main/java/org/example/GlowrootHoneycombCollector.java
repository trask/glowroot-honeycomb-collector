/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.honeycomb.Event;
import io.honeycomb.LibHoney;

import org.glowroot.agent.shaded.glowroot.wire.api.model.AgentConfigOuterClass.AgentConfig;
import org.glowroot.agent.shaded.glowroot.wire.api.model.CollectorServiceOuterClass.Environment;
import org.glowroot.agent.shaded.glowroot.wire.api.model.CollectorServiceOuterClass.GaugeValue;
import org.glowroot.agent.shaded.glowroot.wire.api.model.CollectorServiceOuterClass.LogEvent;
import org.glowroot.agent.shaded.glowroot.wire.api.model.ProfileOuterClass.Profile;
import org.glowroot.agent.shaded.glowroot.wire.api.model.TraceOuterClass.Trace;
import org.glowroot.agent.shaded.glowroot.wire.api.model.TraceOuterClass.Trace.Entry;
import org.glowroot.agent.shaded.glowroot.wire.api.model.TraceOuterClass.Trace.ThreadStats;
import org.glowroot.agent.shaded.slf4j.Logger;
import org.glowroot.agent.shaded.slf4j.LoggerFactory;

public class GlowrootHoneycombCollector implements org.glowroot.agent.collector.Collector {

    private static final String WRITE_KEY = System.getProperty("honeycomb.writeKey");
    private static final String DATA_SET = System.getProperty("honeycomb.dataSet");

    private static final Logger logger = LoggerFactory.getLogger(GlowrootHoneycombCollector.class);

    private volatile LibHoney libhoney;

    @Override
    public void init(File glowrootDir, File agentDir, Environment environment,
            AgentConfig agentConfig, AgentConfigUpdater agentConfigUpdater) {
        libhoney = new LibHoney.Builder()
                .writeKey(WRITE_KEY)
                .dataSet(DATA_SET)
                .build();
        libhoney.addField("server", environment.getHostInfo().getHostName());
    }

    @Override
    public void collectAggregates(AggregateReader aggregateReader) {}

    @Override
    public void collectGaugeValues(List<GaugeValue> gaugeValues) {}

    @Override
    public void collectTrace(TraceReader traceReader) {
        try {
            Event event = libhoney.newEvent();

            Trace.Header header = getHeader(traceReader);

            event.addField("duration nanos", header.getDurationNanos());
            event.addField("transaction type", header.getTransactionType());
            event.addField("transaction name", header.getTransactionName());
            event.addField("headline", header.getHeadline());
            event.addField("user", header.getUser());

            for (Trace.Attribute attribute : header.getAttributeList()) {
                List<String> values = attribute.getValueList();
                if (values.size() == 1) {
                    event.addField(attribute.getName(), values.get(0));
                } else {
                    event.addField(attribute.getName(), values);
                }
            }

            event.addField("error", header.hasError());

            if (header.hasMainThreadRootTimer()) {
                Map<String, Long> timerExclusiveNanos = new HashMap<String, Long>();
                Trace.Timer rootTimer = header.getMainThreadRootTimer();
                recurse(rootTimer, timerExclusiveNanos);
                event.add(timerExclusiveNanos);
            }

            if (header.hasMainThreadStats()) {
                ThreadStats threadStats = header.getMainThreadStats();
                if (threadStats.hasTotalCpuNanos()) {
                    event.addField("total cpu nanos", threadStats.getTotalCpuNanos());
                }
                if (threadStats.hasTotalBlockedNanos()) {
                    event.addField("total blocked nanos", threadStats.getTotalBlockedNanos());
                }
                if (threadStats.hasTotalWaitedNanos()) {
                    event.addField("total waited nanos", threadStats.getTotalWaitedNanos());
                }
            }

            event.send();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void recurse(Trace.Timer timer, Map<String, Long> timerExclusiveNanos) {
        List<Trace.Timer> childTimers = timer.getChildTimerList();
        long totalChildNanos = 0;
        for (Trace.Timer childTimer : childTimers) {
            recurse(childTimer, timerExclusiveNanos);
            totalChildNanos += childTimer.getTotalNanos();
        }
        long currExclusiveNanos = timer.getTotalNanos() - totalChildNanos;
        Long totalExclusiveNanos = timerExclusiveNanos.get(timer.getName());
        if (totalExclusiveNanos == null) {
            timerExclusiveNanos.put(timer.getName(), currExclusiveNanos);
        } else {
            timerExclusiveNanos.put(timer.getName(), totalExclusiveNanos + currExclusiveNanos);
        }
    }

    @Override
    public void log(LogEvent logEvent) {}

    private static Trace.Header getHeader(TraceReader traceReader) throws Exception {
        ExtractingTraceVisitor traceVisitor = new ExtractingTraceVisitor();
        traceReader.accept(traceVisitor);
        return traceVisitor.getHeader();
    }

    private static class ExtractingTraceVisitor implements TraceVisitor {

        private Trace.Header header;

        Trace.Header getHeader() {
            return header;
        }

        @Override
        public int visitSharedQueryText(String sharedQueryText) {
            return 0;
        }

        @Override
        public void visitEntry(Entry entry) {}

        @Override
        public void visitMainThreadProfile(Profile profile) {}

        @Override
        public void visitHeader(Trace.Header header) {
            this.header = header;
        }

        @Override
        public void visitAuxThreadProfile(Profile profile) {}
    }
}
