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

import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.query.api.execution.query.input.state.LogicalStateElement;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gobinath on 5/21/17.
 */
public class LogicalPostStateProcessor extends StreamPostStateProcessor {

    /**
     * Logical type of this processor.
     */
    private LogicalStateElement.Type logicalType;

    /**
     * List of inner {@link StreamPostStateProcessor}s which are part of this logical operation.
     */
    private List<StreamPostStateProcessor> streamPostStateProcessors = new LinkedList<>();

    public LogicalPostStateProcessor(LogicalStateElement.Type logicalType) {
        this.logicalType = logicalType;
    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void setEndOfEvery(boolean endOfEvery) {
        super.setEndOfEvery(endOfEvery);
        for (StreamPostStateProcessor processor : this.streamPostStateProcessors) {
            processor.setEndOfEvery(endOfEvery);
        }
    }


    public void addStreamPostStateProcessor(StreamPostStateProcessor processor) {
        this.streamPostStateProcessors.add(processor);
    }

    /**
     * Clone a copy of processor
     *
     * @param key partition key
     * @return clone of PostStateProcessor
     */
    @Override
    public PostStateProcessor cloneProcessor(String key) {
        LogicalPostStateProcessor logicalPostStateProcessor = new LogicalPostStateProcessor(logicalType);
        cloneProperties(logicalPostStateProcessor);
        return logicalPostStateProcessor;
    }


    @Override
    public boolean isEventReturned() {
        if (nextStatePerProcessor != null) {
            return false;
        }
        if (logicalType == LogicalStateElement.Type.OR) {
            for (StreamPostStateProcessor processor : streamPostStateProcessors) {
                if (!processor.newAndEveryStateEventList.isEmpty()) {
                    processor.newAndEveryStateEventList.clear();
                    return true;
                }
            }
            return false;
        } else if (logicalType == LogicalStateElement.Type.AND) {
            boolean consume = true;
            for (StreamPostStateProcessor processor : streamPostStateProcessors) {
                if (processor.newAndEveryStateEventList.isEmpty() && processor.pendingStateEventList.isEmpty()) {
                    consume = false;
                    break;
                }
            }
            return consume;
        }
        return false;
    }


    /**
     * Returns a {@link List} of {@link StateEvent}s stored in the processor.
     *
     * @return a {@link List} of {@link StateEvent}s
     */
    @Override
    public List<StateEvent> events() {
        List<StateEvent> events = new LinkedList<>();
        if (logicalType == LogicalStateElement.Type.OR) {
            for (StreamPostStateProcessor processor : streamPostStateProcessors) {
                if (!processor.newAndEveryStateEventList.isEmpty()) {
                    events = processor.newAndEveryStateEventList;
                    break;
                }
            }
        } else {
            // AND
            events = streamPostStateProcessors.get(streamPostStateProcessors.size() - 1).newAndEveryStateEventList;
        }
        return events;
    }

}
