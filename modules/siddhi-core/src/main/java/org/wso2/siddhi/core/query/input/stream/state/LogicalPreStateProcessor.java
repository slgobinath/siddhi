/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.query.api.execution.query.input.state.LogicalStateElement;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by gobinath on 5/21/17.
 */
public class LogicalPreStateProcessor extends StreamPreStateProcessor {

    private LogicalStateElement.Type logicalType;

    private List<StreamPreStateProcessor> streamPreStateProcessors = new LinkedList<>();

    public LogicalPreStateProcessor(LogicalStateElement.Type type, StateInputStream.Type stateType, List<Map
            .Entry<Long, Set<Integer>>> withinStates) {
        super(stateType, withinStates);
        this.logicalType = type;
    }

    @Override
    public void init() {
        super.init();
    }

    /**
     * Clone a copy of processor
     *
     * @param key partition key
     * @return clone of LogicalPreStateProcessor
     */
    @Override
    public PreStateProcessor cloneProcessor(String key) {
        LogicalPreStateProcessor logicalPreStateProcessor = new LogicalPreStateProcessor(logicalType, stateType,
                withinStates);
        cloneProperties(logicalPreStateProcessor, key);
        logicalPreStateProcessor.init(executionPlanContext, queryName);
        return logicalPreStateProcessor;
    }

    @Override
    public void setPreviousStatePostProcessor(PostStateProcessor postStateProcessor) {
        super.setPreviousStatePostProcessor(postStateProcessor);

        if (logicalType == LogicalStateElement.Type.OR) {
            for (StreamPreStateProcessor processor : streamPreStateProcessors) {
                processor.setPreviousStatePostProcessor(getPreviousStatePostProcessor());
            }
        } else {
            // AND
            streamPreStateProcessors.get(0).setPreviousStatePostProcessor(getPreviousStatePostProcessor());
            for (int i = 1; i < streamPreStateProcessors.size(); i++) {
                streamPreStateProcessors.get(i).setPreviousStatePostProcessor(streamPreStateProcessors.get(i - 1)
                        .thisStatePostProcessor);
            }
        }
    }

    public void addStreamPreStateProcessor(StreamPreStateProcessor processor) {
        this.streamPreStateProcessors.add(processor);
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        throw new RuntimeException("Cannot handle");
    }
}
