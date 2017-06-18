/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.stream.output.sink;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.core.util.transport.DynamicOptions;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Abstract parent class to represent event mappers. Events mappers will receive {@link Event}s and can convert them
 * to any desired object type (Ex: XML, JSON). Custom mappers can be implemented as extensions extending this
 * abstract implementation.
 */
public abstract class SinkMapper implements Snapshotable {
    private String type;
    private AtomicLong lastEventId = new AtomicLong(Long.MIN_VALUE);
    private String elementId;
    private OptionHolder optionHolder;
    private TemplateBuilder payloadTemplateBuilder = null;
    private OutputGroupDeterminer groupDeterminer = null;

    public final void init(StreamDefinition streamDefinition,
                           String type,
                           OptionHolder mapOptionHolder,
                           String unmappedPayload,
                           ConfigReader mapperConfigReader, SiddhiAppContext siddhiAppContext) {
        this.optionHolder = mapOptionHolder;
        this.type = type;
        if (unmappedPayload != null && !unmappedPayload.isEmpty()) {
            payloadTemplateBuilder = new TemplateBuilder(streamDefinition, unmappedPayload);
        }
        this.elementId = siddhiAppContext.getElementIdGenerator().createNewId();
        init(streamDefinition, mapOptionHolder, payloadTemplateBuilder, mapperConfigReader);
        siddhiAppContext.getSnapshotService().addSnapshotable(streamDefinition.getId() + ".sink.mapper",
                                                                  this);
    }

    /**
     * Updates the {@link Event#id}
     *
     * @param event event to be updated
     */
    private void updateEventId(Event event) {
        // event id -1 is reserved for the events that are arriving for the first Siddhi node
        event.setId(lastEventId.incrementAndGet() == -1 ? lastEventId.incrementAndGet() : lastEventId.get());
    }

    /**
     * Update the {@link Event#id}s
     *
     * @param events events to be updated
     */
    private void updateEventIds(Event[] events) {
        for (Event event : events) {
            // event id -1 is reserved for the events that are arriving for the first Siddhi node
            event.setId(lastEventId.incrementAndGet() == -1 ? lastEventId.incrementAndGet() : lastEventId.get());
        }
    }

    /**
     * Supported dynamic options by the mapper
     *
     * @return the list of supported dynamic option keys
     */
    public abstract String[] getSupportedDynamicOptions();


    /**
     * Initialize the mapper and the mapping configurations.
     *  @param streamDefinition       The stream definition
     * @param optionHolder           Option holder containing static and dynamic options related to the mapper
     * @param payloadTemplateBuilder un mapped payload for reference
     * @param mapperConfigReader this hold the {@link SinkMapper} extensions configuration reader.
     */
    public abstract void init(StreamDefinition streamDefinition,
                              OptionHolder optionHolder, TemplateBuilder payloadTemplateBuilder, ConfigReader
                                      mapperConfigReader);

    /**
     * Called to map the events and send them to {@link SinkListener}
     *
     * @param events                  {@link Event}s that need to be mapped
     * @param sinkListener {@link SinkListener} that will be called with the mapped events
     * @throws ConnectionUnavailableException If the connection is not available to send the message
     */
    public void mapAndSend(Event[] events, SinkListener sinkListener)
            throws ConnectionUnavailableException {
        updateEventIds(events);
        if (groupDeterminer != null) {
            LinkedHashMap<String, ArrayList<Event>> eventMap = new LinkedHashMap<>();
            for (Event event : events) {
                String key = groupDeterminer.decideGroup(event);
                ArrayList<Event> eventList = eventMap.computeIfAbsent(key, k -> new ArrayList<>());
                eventList.add(event);
            }

            for (ArrayList<Event> eventList : eventMap.values()) {
                mapAndSend(eventList.toArray(new Event[eventList.size()]), optionHolder,
                           payloadTemplateBuilder, sinkListener, new DynamicOptions(eventList.get(0)));

            }
        } else {
            mapAndSend(events, optionHolder, payloadTemplateBuilder, sinkListener,
                       new DynamicOptions(events[0]));
        }
    }

    /**
     * Called to map the event and send it to {@link SinkListener}
     *
     * @param event                   The {@link Event} that need to be mapped
     * @param sinkListener {@link SinkListener} that will be called with the mapped event
     * @throws ConnectionUnavailableException If the connection is not available to send the message
     */
    public void mapAndSend(Event event, SinkListener sinkListener)
            throws ConnectionUnavailableException {
        updateEventId(event);
        mapAndSend(event, optionHolder, payloadTemplateBuilder, sinkListener, new DynamicOptions(event));
    }

    /**
     * Called to map the events and send them to {@link SinkListener}
     *
     * @param events                  {@link Event}s that need to be mapped
     * @param optionHolder            Option holder containing static and dynamic options related to the mapper
     * @param payloadTemplateBuilder  To build the message payload based on the given template
     * @param sinkListener {@link SinkListener} that will be called with the mapped events
     * @param dynamicOptions          {@link DynamicOptions} containing transport related options which will be passed
     *                                to the  {@link SinkListener}
     * @throws ConnectionUnavailableException If the connection is not available to send the message
     */
    public abstract void mapAndSend(Event[] events, OptionHolder optionHolder, TemplateBuilder payloadTemplateBuilder,
                                    SinkListener sinkListener, DynamicOptions dynamicOptions)
            throws ConnectionUnavailableException;

    /**
     * Called to map the event and send it to {@link SinkListener}
     *
     * @param event                   {@link Event} that need to be mapped
     * @param optionHolder            Option holder containing static and dynamic options related to the mapper
     * @param payloadTemplateBuilder  To build the message payload based on the given template
     * @param sinkListener {@link SinkListener} that will be called with the mapped event
     * @param dynamicOptions          {@link DynamicOptions} containing transport related options which will be passed
     *                                to the  {@link SinkListener}
     * @throws ConnectionUnavailableException If the connection is not available to send the message
     */
    public abstract void mapAndSend(Event event, OptionHolder optionHolder, TemplateBuilder payloadTemplateBuilder,
                                    SinkListener sinkListener, DynamicOptions dynamicOptions)
            throws ConnectionUnavailableException;

    public final String getType() {
        return this.type;
    }

    public final void setGroupDeterminer(OutputGroupDeterminer groupDeterminer) {
        this.groupDeterminer = groupDeterminer;
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        state.put("LastEventId", lastEventId);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        lastEventId = (AtomicLong) state.get("LastEventId");
    }

    @Override
    public String getElementId() {
        return elementId;
    }

}
