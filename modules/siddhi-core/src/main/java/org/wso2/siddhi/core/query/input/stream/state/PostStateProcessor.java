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
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;

import java.util.List;

/**
 * A super interface which provides the template of all the post state processors.
 */
public interface PostStateProcessor extends Processor, Snapshotable {

    /**
     * Get the state id of this processor.
     *
     * @return the state id
     */
    int getStateId();

    /**
     * Set the next state processor to this {@link PostStateProcessor}
     *
     * @param nextStatePerProcessor the next processor
     */
    void setNextStatePreProcessor(PreStateProcessor nextStatePerProcessor);

    /**
     * Clone the processor.
     *
     * @param key partition key
     * @return
     */
    PostStateProcessor cloneProcessor(String key);

    /**
     * Mark this processor as the last processor in an every pattern.
     *
     * @param endOfEvery
     */
    void setEndOfEvery(boolean endOfEvery);

    /**
     * Returns a {@link List} of {@link StateEvent}s stored in the processor.
     *
     * @return a {@link List} of {@link StateEvent}s
     */
    List<StateEvent> events();

    /**
     * Make the processed new events available for the next processor.
     */
    void updateState();
}
