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
package org.wso2.siddhi.core.query.input.stream.state.runtime;

import org.wso2.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import org.wso2.siddhi.core.query.input.stream.state.LogicalPostStateProcessor;
import org.wso2.siddhi.core.query.input.stream.state.LogicalPreStateProcessor;
import org.wso2.siddhi.core.query.input.stream.state.StreamPostStateProcessor;
import org.wso2.siddhi.core.query.input.stream.state.StreamPreStateProcessor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.List;

/**
 * Created on 12/19/14.
 */
public class LogicalInnerStateRuntime extends StreamInnerStateRuntime {

    private final InnerStateRuntime innerStateRuntime1;
    private final InnerStateRuntime innerStateRuntime2;

    public LogicalInnerStateRuntime(InnerStateRuntime innerStateRuntime1, InnerStateRuntime innerStateRuntime2,
                                    StateInputStream.Type stateType) {
        super(stateType);
        this.innerStateRuntime1 = innerStateRuntime1;
        this.innerStateRuntime2 = innerStateRuntime2;
    }

    @Override
    public void setQuerySelector(Processor commonProcessor) {
        innerStateRuntime1.setQuerySelector(commonProcessor);
        innerStateRuntime2.setQuerySelector(commonProcessor);
    }

    @Override
    public void setStartState() {
        innerStateRuntime2.setStartState();
        innerStateRuntime1.setStartState();
    }

    @Override
    public void init() {

        innerStateRuntime2.init();
        innerStateRuntime1.init();
    }

    @Override
    public void reset() {
        innerStateRuntime2.reset();
    }

    @Override
    public void update() {
        innerStateRuntime2.update();
    }

    @Override
    public InnerStateRuntime clone(String key) {
        InnerStateRuntime clonedInnerStateRuntime1 = innerStateRuntime1.clone(key);
        InnerStateRuntime clonedInnerStateRuntime2 = innerStateRuntime2.clone(key);

        LogicalPreStateProcessor logicalPreStateProcessor = (LogicalPreStateProcessor)
                firstProcessor.cloneProcessor(key);
        LogicalPostStateProcessor logicalPostStateProcessor = (LogicalPostStateProcessor)
                lastProcessor.cloneProcessor(key);

        logicalPreStateProcessor.setThisStatePostProcessor(logicalPostStateProcessor);

        StreamPreStateProcessor logicalPreStateProcessor1 = (StreamPreStateProcessor)
                clonedInnerStateRuntime1.getFirstProcessor();
        StreamPostStateProcessor logicalPostStateProcessor1 = (StreamPostStateProcessor)
                clonedInnerStateRuntime1.getLastProcessor();
        StreamPreStateProcessor logicalPreStateProcessor2 = (StreamPreStateProcessor)
                clonedInnerStateRuntime2.getFirstProcessor();
        StreamPostStateProcessor logicalPostStateProcessor2 = (StreamPostStateProcessor)
                clonedInnerStateRuntime2.getLastProcessor();

        logicalPreStateProcessor.addStreamPreStateProcessor(logicalPreStateProcessor1);
        logicalPreStateProcessor.addStreamPreStateProcessor(logicalPreStateProcessor2);

        logicalPostStateProcessor.addStreamPostStateProcessor(logicalPostStateProcessor1);
        logicalPostStateProcessor.addStreamPostStateProcessor(logicalPostStateProcessor2);

        logicalPreStateProcessor1.setThisLastProcessor(logicalPostStateProcessor);
        logicalPreStateProcessor2.setThisLastProcessor(logicalPostStateProcessor);

        logicalPreStateProcessor.setNextProcessor(clonedInnerStateRuntime1.getFirstProcessor());

        clonedInnerStateRuntime2.getLastProcessor().setNextProcessor(logicalPostStateProcessor);
        clonedInnerStateRuntime2.setLastProcessor(logicalPostStateProcessor);


        LogicalInnerStateRuntime logicalInnerStateRuntime = new LogicalInnerStateRuntime(clonedInnerStateRuntime1,
                clonedInnerStateRuntime2, stateType);
        logicalInnerStateRuntime.firstProcessor = logicalPreStateProcessor;
        logicalInnerStateRuntime.lastProcessor = logicalPostStateProcessor;

        logicalInnerStateRuntime.getSingleStreamRuntimeList().addAll
                (clonedInnerStateRuntime2.getSingleStreamRuntimeList());
        logicalInnerStateRuntime.getSingleStreamRuntimeList().addAll
                (clonedInnerStateRuntime1.getSingleStreamRuntimeList());

        List<SingleStreamRuntime> runtimeList = logicalInnerStateRuntime.getSingleStreamRuntimeList();
        for (int i = 0; i < runtimeList.size(); i++) {
            String streamId = runtimeList.get(i).getProcessStreamReceiver().getStreamId();
            for (int j = i; j < runtimeList.size(); j++) {
                if (streamId.equals(runtimeList.get(j).getProcessStreamReceiver().getStreamId())) {
                    runtimeList.get(j).setProcessStreamReceiver(runtimeList.get(i).getProcessStreamReceiver());
                }
            }
        }
        return logicalInnerStateRuntime;
    }
}
