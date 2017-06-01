/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created on 1/6/15.
 */
public class CountPreStateProcessor extends StreamPreStateProcessor {
    private final int minCount;
    private final int maxCount;
    protected volatile boolean successCondition = false;
    private List<StateEvent> preEvents = new ArrayList<>();

    public CountPreStateProcessor(int minCount, int maxCount, StateInputStream.Type stateType, List<Map.Entry<Long,
            Set<Integer>>> withinStates) {
        super(stateType, withinStates);
        this.minCount = minCount;
        this.maxCount = maxCount;
    }


    public PreStateProcessor cloneProcessor(String key) {
        CountPreStateProcessor countPreStateProcessor = new CountPreStateProcessor(minCount, maxCount, stateType,
                withinStates);
        cloneProperties(countPreStateProcessor, key);
        countPreStateProcessor.init(executionPlanContext, queryName);
        return countPreStateProcessor;
    }

    @Override
    public void updateState() {
        super.updateState();
    }

    @Override
    protected Iterator<StateEvent> iterator() {
        List<StateEvent> list;
        if (previousStatePostProcessor != null) {
            list = previousStatePostProcessor.events();
        } else {
            list = preEvents;
            if (isStartState && list.isEmpty()) {
                list.add(stateEventPool.borrowEvent());
            }
        }
        return list.iterator();
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {

        // Set the consumed flag to false for every events
        consumedLastEvent = false;
        ComplexEventChunk<StateEvent> returnEventChunk = new ComplexEventChunk<StateEvent>(false);
        complexEventChunk.reset();
        StreamEvent streamEvent = (StreamEvent) complexEventChunk.next(); //Sure only one will be sent
        for (Iterator<StateEvent> iterator = iterator(); iterator.hasNext(); ) {
            StateEvent stateEvent = iterator.next();
            if (removeIfNextStateProcessed(stateEvent, iterator, stateId + 1)) {
                continue;
            }
            if (removeIfNextStateProcessed(stateEvent, iterator, stateId + 2)) {
                continue;
            }
            if (withinStates.size() > 0) {
                if (isExpired(stateEvent, streamEvent)) {
                    iterator.remove();
                    continue;
                }
            }

            StateEvent eventToProcess = stateEvent;
            // The start of every does not remove the events from previous processor.
            // Therefore the state object should not be modified here or later in the next processors.
            if (startOfEvery) {
                eventToProcess = stateEventCloner.copyStateEvent(stateEvent);
            }
            eventToProcess.addEvent(stateId, streamEventCloner.copyStreamEvent(streamEvent));
            successCondition = false;
            process(eventToProcess);
            if (this.thisLastProcessor.isEventReturned()) {
                this.thisLastProcessor.clearProcessedEvent();
                returnEventChunk.add(eventToProcess);
            }
            if (stateChanged) {
                // Remove from the previous processor only if it is not the beginning of an every pattern
                if (!startOfEvery) {
                    iterator.remove();
                }
            }
            if (!successCondition) {
                switch (stateType) {
                    case PATTERN:
                        stateEvent.removeLastEvent(stateId);
                        break;
                    case SEQUENCE:
                        stateEvent.removeLastEvent(stateId);
                        iterator.remove();
                        break;
                }
            }
        }
        return returnEventChunk;
    }

    private boolean removeIfNextStateProcessed(StateEvent stateEvent, Iterator<StateEvent> iterator, int position) {
        if (stateEvent.getStreamEvents().length > position && stateEvent.getStreamEvent(position) != null) {
            iterator.remove();
            return true;
        }
        return false;
    }

    @Override
    public void setThisLastProcessor(StreamPostStateProcessor thisLastProcessor) {
        super.setThisLastProcessor(thisLastProcessor);
    }
}
