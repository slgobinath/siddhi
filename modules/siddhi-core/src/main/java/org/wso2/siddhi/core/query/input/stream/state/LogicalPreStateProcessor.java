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
 * The processor that comes before the individual {@link StreamPreStateProcessor}s of a logical pattern.
 * This processor sets the {@link #previousStatePostProcessor} of all individual processors.
 */
public class LogicalPreStateProcessor extends StreamPreStateProcessor {

    /**
     * Logical type of the processor.
     */
    private LogicalStateElement.Type logicalType;

    /**
     * {@link StreamPreStateProcessor}s which are part of the logical pattern.
     */
    private List<StreamPreStateProcessor> streamPreStateProcessors = new LinkedList<>();

    /**
     * Create a LogicalPreStateProcessor instance.
     *
     * @param logicalType  the logical type
     * @param stateType    the state type
     * @param withinStates the within conditions
     */
    public LogicalPreStateProcessor(LogicalStateElement.Type logicalType, StateInputStream.Type stateType,
                                    List<Map.Entry<Long, Set<Integer>>> withinStates) {
        super(stateType, withinStates);
        this.logicalType = logicalType;
    }

    /**
     * Clone a copy of processor
     *
     * @param key partition key
     * @return clone of LogicalPreStateProcessor
     */
    @Override
    public PreStateProcessor cloneProcessor(String key) {
        LogicalPreStateProcessor processor = new LogicalPreStateProcessor(logicalType, stateType, withinStates);
        cloneProperties(processor, key);
        processor.init(siddhiAppContext, queryName);
        return processor;
    }

    /**
     * Set the previous {@link PostStateProcessor} of the inner {@link StreamPreStateProcessor}s.
     *
     * @param postStateProcessor the previous PostStateProcessor
     */
    @Override
    public void setPreviousStatePostProcessor(PostStateProcessor postStateProcessor) {
        // Set the previousStatePostProcessor to this processor
        super.setPreviousStatePostProcessor(postStateProcessor);

        if (logicalType == LogicalStateElement.Type.OR) {
            // In OR operation all the processors are parallel to each other
            // So they share the same previousStatePostProcessor.
            //                              |->  streamPreStateProcessor1
            // previousStatePostProcessor - |->  streamPreStateProcessor2
            //                              |->  streamPreStateProcessor3
            for (StreamPreStateProcessor processor : streamPreStateProcessors) {
                processor.setPreviousStatePostProcessor(getPreviousStatePostProcessor());
            }
        } else {
            // In AND operation, processors are aligned dynamically.
            // previousStatePostProcessor -> streamPreStateProcessor(which receives the event first) ->
            // streamPreStateProcessor(which receives the event next) ->

            for (StreamPreStateProcessor processor : streamPreStateProcessors) {
                processor.setPreviousStatePostProcessor(new ANDPreviousProcessor(processor));
            }
        }
    }

    /**
     * Add the {@link StreamPreStateProcessor} to the logical pattern.
     *
     * @param processor processor of the logical pattern
     */
    public void addStreamPreStateProcessor(StreamPreStateProcessor processor) {
        this.streamPreStateProcessors.add(processor);
    }

    /**
     * {@link LogicalPreStateProcessor} is meant to share the previous processor the inner processors of the logical
     * pattern. This processor is not intended to process any events.
     *
     * @param complexEventChunk
     * @return
     * @throws RuntimeException always throws an exception
     */
    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        throw new RuntimeException("LogicalPreStateProcessor cannot process any events");
    }

    private class ANDPreviousProcessor extends StreamPostStateProcessor {
        private PreStateProcessor preStateProcessor;

        public ANDPreviousProcessor(PreStateProcessor preStateProcessor) {
            this.preStateProcessor = preStateProcessor;
        }

        @Override
        public List<StateEvent> events() {
            for (StreamPreStateProcessor processor : LogicalPreStateProcessor.this.streamPreStateProcessors) {
                if (processor != preStateProcessor && !processor.thisStatePostProcessor.newAndEveryStateEventList
                        .isEmpty()) {
                    return processor.thisStatePostProcessor.newAndEveryStateEventList;
                }
            }
            return LogicalPreStateProcessor.this.getPreviousStatePostProcessor().events();
        }

        @Override
        public void updateState() {
            LogicalPreStateProcessor.this.getPreviousStatePostProcessor().updateState();
        }
    }
}
