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
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.LinkedList;
import java.util.List;

/**
 * Created on 12/17/14. Make this snapshotable.
 */
public class StreamPostStateProcessor implements PostStateProcessor {
    protected PreStateProcessor nextStatePerProcessor;
    protected StreamPreStateProcessor thisStatePreProcessor;
    protected Processor nextProcessor;
    protected int stateId;
    protected boolean isEventReturned;

    /**
     * List of newly arrived process events.
     */
    protected List<StateEvent> newAndEveryStateEventList = new LinkedList<>();

    /**
     * List of processed events moved from {@link StreamPostStateProcessor#newAndEveryStateEventList}.
     */
    protected List<StateEvent> pendingStateEventList = new LinkedList<>();

    /**
     * A flag indicates whether this is the last processor in an every pattern.
     */
    protected boolean endOfEvery = false;

    /**
     * Process the handed StreamEvent
     *
     * @param complexEventChunk event chunk to be processed
     */
    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        if (complexEventChunk.hasNext()) {     //one one event will be coming
            StateEvent stateEvent = (StateEvent) complexEventChunk.next();
            process(stateEvent, complexEventChunk);
        }
        complexEventChunk.clear();
    }

    protected void process(StateEvent stateEvent, ComplexEventChunk complexEventChunk) {
        thisStatePreProcessor.stateChanged();

        StreamEvent streamEvent = stateEvent.getStreamEvent(stateId);
        stateEvent.setTimestamp(streamEvent.getTimestamp());

        if (nextProcessor != null) {
            complexEventChunk.reset();
            this.isEventReturned = true;
        }

        if (thisStatePreProcessor.stateType == StateInputStream.Type.PATTERN) {
            if (endOfEvery) {
                newAndEveryStateEventList.add(stateEvent);
                thisStatePreProcessor.setConsumedLastEvent(!thisStatePreProcessor.startOfEvery);
            } else if (newAndEveryStateEventList.isEmpty() && pendingStateEventList.isEmpty()) {
                newAndEveryStateEventList.add(stateEvent);
                thisStatePreProcessor.setConsumedLastEvent(false);
            }
        } else {
            // SEQUENCE
            pendingStateEventList.clear();
            newAndEveryStateEventList.clear();
            newAndEveryStateEventList.add(stateEvent);
            thisStatePreProcessor.setConsumedLastEvent(false);
        }
    }

    public boolean isEventReturned() {
        return isEventReturned;
    }

    public void clearProcessedEvent() {
        isEventReturned = false;
    }

    /**
     * Get next processor element in the processor chain. Processed event should be sent to next processor
     *
     * @return next processor
     */
    @Override
    public Processor getNextProcessor() {
        return nextProcessor;
    }

    /**
     * Set next processor element in processor chain
     *
     * @param nextProcessor Processor to be set as next element of processor chain
     */
    @Override
    public void setNextProcessor(Processor nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    /**
     * Set as the last element of the processor chain
     *
     * @param processor Last processor in the chain
     */
    @Override
    public void setToLast(Processor processor) {
        if (nextProcessor == null) {
            this.nextProcessor = processor;
        } else {
            this.nextProcessor.setToLast(processor);
        }
    }

    /**
     * Clone a copy of processor
     *
     * @param key partition key
     * @return clone of StreamPostStateProcessor
     */
    @Override
    public PostStateProcessor cloneProcessor(String key) {
        StreamPostStateProcessor streamPostStateProcessor = new StreamPostStateProcessor();
        cloneProperties(streamPostStateProcessor);
        return streamPostStateProcessor;
    }

    /**
     * Mark this processor as the last processor in an every pattern.
     *
     * @param endOfEvery
     */
    @Override
    public void setEndOfEvery(boolean endOfEvery) {
        this.endOfEvery = endOfEvery;
    }

    /**
     * Returns a {@link List} of {@link StateEvent}s stored in the processor.
     *
     * @return a {@link List} of {@link StateEvent}s
     */
    public List<StateEvent> events() {
        return this.pendingStateEventList;
    }

    /**
     * Make the processed new events available for the next processor.
     */
    @Override
    public void updateState() {
        pendingStateEventList.addAll(newAndEveryStateEventList);
        newAndEveryStateEventList.clear();
    }

    protected void cloneProperties(StreamPostStateProcessor streamPostStateProcessor) {
        streamPostStateProcessor.stateId = this.stateId;
        streamPostStateProcessor.endOfEvery = this.endOfEvery;
    }

    @Override
    public void setNextStatePreProcessor(PreStateProcessor preStateProcessor) {
        this.nextStatePerProcessor = preStateProcessor;

        // Set this as the previous post state processor
        preStateProcessor.setPreviousStatePostProcessor(this);
    }

    public void setThisStatePreProcessor(StreamPreStateProcessor preStateProcessor) {
        thisStatePreProcessor = preStateProcessor;
    }

    public int getStateId() {
        return stateId;
    }

    public void setStateId(int stateId) {
        this.stateId = stateId;
    }
}
