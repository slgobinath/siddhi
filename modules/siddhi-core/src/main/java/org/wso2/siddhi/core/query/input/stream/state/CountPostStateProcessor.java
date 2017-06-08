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

import java.util.LinkedList;
import java.util.List;

/**
 * Created on 1/6/15.
 */
public class CountPostStateProcessor extends StreamPostStateProcessor {
    private final int minCount;
    private final int maxCount;

    public CountPostStateProcessor(int minCount, int maxCount) {
        this.minCount = minCount;
        this.maxCount = maxCount;
    }

    public PostStateProcessor cloneProcessor(String key) {
        CountPostStateProcessor countPostStateProcessor = new CountPostStateProcessor(minCount, maxCount);
        cloneProperties(countPostStateProcessor);
        return countPostStateProcessor;
    }

    protected void process(StateEvent stateEvent, ComplexEventChunk complexEventChunk) {

        ((CountPreStateProcessor) thisStatePreProcessor).successCondition = true;

        if (thisStatePreProcessor.stateType == StateInputStream.Type.SEQUENCE) {
            thisStatePreProcessor.setConsumedLastEvent(true);
            if ((!endOfEvery || maxCount == 1)) {
                pendingStateEventList.clear();
            }
        }
        newAndEveryStateEventList.clear();
        newAndEveryStateEventList.add(stateEvent);

        int streamEvents = getStreamCount(newAndEveryStateEventList);

        if (nextProcessor != null && streamEvents >= minCount) {
            thisStatePreProcessor.stateChanged();
        }
    }

    @Override
    public boolean isEventReturned() {
        if (nextProcessor != null) {
            if (getStreamCount(newAndEveryStateEventList) >= minCount) {
                super.updateState();
                return true;
            }
        }
        return false;
    }

    @Override
    public void updateState() {
        if (getStreamCount(newAndEveryStateEventList) >= minCount) {
            for (StateEvent stateEvent : newAndEveryStateEventList) {
                if (!pendingStateEventList.contains(stateEvent)) {
                    pendingStateEventList.add(stateEvent);
                }
            }
            newAndEveryStateEventList.clear();
        }
    }

    private int getStreamCount(List<StateEvent> events) {
        int streamEvents;
        if (events.size() == 1) {

            if (events.isEmpty()) {
                streamEvents = 0;
            } else {
                StreamEvent streamEvent = events.get(0).getStreamEvent(stateId);
                if (streamEvent == null) {
                    streamEvents = 0;
                } else {
                    streamEvents = 1;
                    while (streamEvent.getNext() != null) {
                        streamEvents++;
                        streamEvent = streamEvent.getNext();
                    }
                }
            }
        } else {
            streamEvents = events.size();
        }

        return streamEvents;
    }

    /**
     * Returns a {@link List} of {@link StateEvent}s stored in the processor.
     *
     * @return a {@link List} of {@link StateEvent}s
     */
    @Override
    public List<StateEvent> events() {

        List<StateEvent> events = pendingStateEventList;
        if (events.isEmpty()) {
            List<StateEvent> list;
            if (minCount == 0) {
                if (thisStatePreProcessor.getPreviousStatePostProcessor() != null) {
                    list = thisStatePreProcessor.getPreviousStatePostProcessor().events();
                } else {
                    list = new LinkedList<>();

                    if (thisStatePreProcessor.isStartState) {
                        list.add(thisStatePreProcessor.stateEventPool.borrowEvent());
                    }
                }
            } else {
                list = events;
            }
            return list;

        } else {
            int streamEvents = getStreamCount(events);
            if (streamEvents >= minCount) {
                return events;
            } else {
                return new LinkedList<>();
            }
        }
    }
}
