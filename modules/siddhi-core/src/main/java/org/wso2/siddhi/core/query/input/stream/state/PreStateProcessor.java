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
import org.wso2.siddhi.core.query.processor.Processor;

/**
 * Created on 12/17/14.
 */
public interface PreStateProcessor extends Processor {

    ComplexEventChunk processAndReturn(ComplexEventChunk complexEventChunk);

    int getStateId();

    void setStateId(int stateId);

    void init();

    void setStartState(boolean isStartState);

    void updateState();

    StreamPostStateProcessor getThisStatePostProcessor();

    void resetState();

    PreStateProcessor cloneProcessor(String key);

    /**
     * Set this processor as the starting state of an every pattern.
     *
     * @param isStartEvery
     */
    void setStartOfEvery(boolean isStartEvery);

    /**
     * Returns whether the last event was consumed by this processor or not.
     *
     * @return true if the last event was consumed by this processor otherwise false.
     */
    boolean hasConsumedLastEvent();

    PostStateProcessor getPreviousStatePostProcessor();

    /**
     * Set the previous {@link PostStateProcessor} of this processor.
     *
     * @param postStateProcessor the previous PostStateProcessor
     */
    void setPreviousStatePostProcessor(PostStateProcessor postStateProcessor);

}
