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

import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.Iterator;
import java.util.LinkedList;
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
    private CountPostStateProcessor countPostStateProcessor;
    private volatile boolean startStateReset = false;

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
            if (thisStatePostProcessor.pendingStateEventList.isEmpty()) {
                list = previousStatePostProcessor.events();
            } else {
                list = new LinkedList<>();
                StateEvent stateEvent = stateEventCloner.copyStateEvent(thisStatePostProcessor.pendingStateEventList
                        .get(0));
                stateEvent.setEvent(stateId, null);
                list.add(stateEvent);
            }
        } else {
            list = new LinkedList<>();
            if (isStartState) {
                list.add(stateEventPool.borrowEvent());
            }
        }
        return list.iterator();
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
            thisStatePostProcessor.pendingStateEventList.clear();
        }
    }

    public void startStateReset() {
        startStateReset = true;
        if (thisStatePostProcessor.callbackPreStateProcessor != null) {
            ((CountPreStateProcessor) countPostStateProcessor.thisStatePreProcessor).startStateReset();
        }
    }

    //
//    @Override
//    public void updateState() {
//        if (startStateReset) {
//            startStateReset = false;
//            init();
//        }
//        super.updateState();
//    }
}
