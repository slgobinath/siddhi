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
package org.wso2.siddhi.core.dynamic.deployment;


import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.SiddhiApp;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.condition.Compare;

public class SimpleQueryDeploymentTestCase {


    @Test
    public void testCreatingFilterQuery() throws InterruptedException {

        StreamDefinition streamDefinition = StreamDefinition.id("StockStream")
                .attribute("symbol", Attribute.Type.STRING)
                .attribute("price", Attribute.Type.FLOAT)
                .attribute("volume", Attribute.Type.INT);
        Query oldQuery = Query.query();
        oldQuery.annotation(Annotation.annotation("info").element("name", "query1"));
        oldQuery.from(
                InputStream.stream("StockStream").
                        filter(
                                Expression.compare(
                                        Expression.variable("volume"),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.value(100)
                                )
                        )
        );
        oldQuery.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))
        );
        oldQuery.insertInto("OutStockStream");

        SiddhiApp executionPlan = SiddhiApp.siddhiApp("Test")
                .defineStream(streamDefinition)
                .addQuery(oldQuery);

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(executionPlan);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"IBM", 700f, 0});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 500});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 80.5f, 600});
        Thread.sleep(100);




//        Query newQuery = Query.query();
//        newQuery.annotation(Annotation.annotation("info").element("name", "query1"));
//        newQuery.from(
//                InputStream.stream("StockStream").
//                        filter(
//                                Expression.compare(
//                                        Expression.variable("volume"),
//                                        Compare.Operator.GREATER_THAN_EQUAL,
//                                        Expression.value(50)
//                                )
//                        )
//        );
//        newQuery.select(
//                Selector.selector().
//                        select("symbol", Expression.variable("symbol")).
//                        select("price", Expression.variable("price")).
//                        select("volume", Expression.variable("volume"))
//        );
//        newQuery.insertInto("OutStockStream");
//
//        inputHandler.send(new Object[]{"IBM", 700f, 150});
//        Thread.sleep(100);
//        inputHandler.send(new Object[]{"WSO2", 60.5f, 200});
//        Thread.sleep(100);

        siddhiAppRuntime.shutdown();

    }


}
