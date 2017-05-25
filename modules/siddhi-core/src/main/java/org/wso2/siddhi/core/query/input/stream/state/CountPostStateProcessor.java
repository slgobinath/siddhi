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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created on 1/6/15.
 */
public class CountPostStateProcessor extends StreamPostStateProcessor {
    private final int minCount;
    private final int maxCount;
    protected StreamEvent lastEvent;

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


        if (pendingStateEventList.isEmpty()) {
            if (newAndEveryStateEventList.isEmpty()) {
                newAndEveryStateEventList.add(stateEvent);
            } else {
                newAndEveryStateEventList.get(0).addEvent(stateId, stateEvent.getStreamEvent(stateId));
                newAndEveryStateEventList.get(0).setTimestamp(stateEvent.getStreamEvent(stateId).getTimestamp());
            }
        } else {
            newAndEveryStateEventList.clear();
            newAndEveryStateEventList.add(pendingStateEventList.get(0));
            newAndEveryStateEventList.get(0).addEvent(stateId, stateEvent.getStreamEvent(stateId));
            newAndEveryStateEventList.get(0).setTimestamp(stateEvent.getStreamEvent(stateId).getTimestamp());
            pendingStateEventList.clear();
        }
        int streamEvents = getStreamCount(newAndEveryStateEventList);

        if (streamEvents < minCount) {
            thisStatePreProcessor.setConsumedLastEvent(true);
        } else {
            thisStatePreProcessor.setConsumedLastEvent(false);
        }
        thisStatePreProcessor.stateChanged();

    }

    @Override
    public boolean isEventReturned() {
        int limit = minCount;
        if (limit == 0) {
            limit = maxCount;
        }
        if (getStreamCount(newAndEveryStateEventList) >= limit) {
            super.updateState();
            return true;
        }
        return false;
    }

    @Override
    public void updateState() {
        if (getStreamCount(newAndEveryStateEventList) >= minCount) {
//            for (StateEvent event : newAndEveryStateEventList) {
//                if (pendingStateEventList.isEmpty()) {
//                    pendingStateEventList.add(event);
//                } else {
//                    System.out.println("---------------------------------------");
//                    pendingStateEventList.get(0).addEvent(stateId, event.getStreamEvent(stateId));
//                    pendingStateEventList.get(0).setTimestamp(event.getStreamEvent(stateId).getTimestamp());
//                }
//            }
//            newAndEveryStateEventList.clear();
            super.updateState();
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
                if (newAndEveryStateEventList.size() == 1) {
                    list = new MyList();
                    StateEvent stateEvent = cloneEvent(newAndEveryStateEventList.get(0));
                    list.add(stateEvent);
                } else if (thisStatePreProcessor.getPreviousStatePostProcessor() != null) {
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

    private StateEvent cloneEvent(StateEvent event) {
        StateEvent newSateEvent = thisStatePreProcessor.stateEventCloner.copyStateEvent(event);
        newSateEvent.setEvent(stateId, null);
        StreamEvent oldStreamEvent = event.getStreamEvent(stateId);

        while (oldStreamEvent.getNext() != null) {
            newSateEvent.addEvent(stateId, thisStatePreProcessor.streamEventCloner.copyStreamEvent(oldStreamEvent));
            oldStreamEvent = oldStreamEvent.getNext();
        }
        return newSateEvent;
    }


    private class MyList extends LinkedList<StateEvent> {
        @Override
        public Iterator<StateEvent> iterator() {
            return new MyIterator(super.iterator());
        }
    }

    private class MyIterator implements Iterator<StateEvent> {
        private Iterator<StateEvent> iterator;

        public MyIterator(Iterator<StateEvent> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public StateEvent next() {
            return iterator.next();
        }

        @Override
        public void remove() {
            iterator.remove();
            newAndEveryStateEventList.clear();
//            thisStatePreProcessor.previousStatePostProcessor.events().clear();
        }
    }
}
