package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.query.api.execution.query.input.state.LogicalStateElement;
import org.wso2.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by gobinath on 5/21/17.
 */
public class EveryPreStateProcessor extends StreamPreStateProcessor {

    private LogicalStateElement.Type logicalType;

    private List<StreamPreStateProcessor> streamPreStateProcessors = new LinkedList<>();

    public EveryPreStateProcessor(LogicalStateElement.Type type, StateInputStream.Type stateType, List<Map
            .Entry<Long, Set<Integer>>> withinStates) {
        super(stateType, withinStates);
        this.logicalType = type;
    }

    @Override
    public void setPreviousStatePostProcessor(PostStateProcessor postStateProcessor) {
        super.setPreviousStatePostProcessor(postStateProcessor);

        if (logicalType == LogicalStateElement.Type.OR) {
            for (StreamPreStateProcessor processor : streamPreStateProcessors) {
                processor.setPreviousStatePostProcessor(getPreviousStatePostProcessor());
            }
        } else {
            // AND
            streamPreStateProcessors.get(0).setPreviousStatePostProcessor(getPreviousStatePostProcessor());
            for (int i = 1; i < streamPreStateProcessors.size(); i++) {
                streamPreStateProcessors.get(i).setPreviousStatePostProcessor(streamPreStateProcessors.get(i - 1)
                        .thisStatePostProcessor);
            }
        }
    }

    public void addStreamPreStateProcessor(StreamPreStateProcessor processor) {
        this.streamPreStateProcessors.add(processor);
    }

    @Override
    public ComplexEventChunk<StateEvent> processAndReturn(ComplexEventChunk complexEventChunk) {
        throw new RuntimeException("Cannot handle");
    }
}
