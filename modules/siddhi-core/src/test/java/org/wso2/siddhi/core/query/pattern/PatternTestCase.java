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

package org.wso2.siddhi.core.query.pattern;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;


@SuppressWarnings("Duplicates")
public class PatternTestCase {

    private static final Logger log = Logger.getLogger(PatternTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void testQuery1() throws InterruptedException {
        log.info("testPatternEvery1 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> e2=Stream2[price>e1.price] " +
                "select e1.symbol as symbol1, e2.symbol as symbol2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    Assert.assertArrayEquals(new Object[]{"WSO2", "IBM"}, inEvents[0].getData());
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQuery2() throws InterruptedException {
        log.info("testPatternEvery2 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price1 float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> e2=Stream2[price1>e1.price] " +
                "select e1.symbol as symbol1, e2.symbol as symbol2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    Assert.assertArrayEquals(new Object[]{"WSO2", "IBM"}, inEvents[0].getData());
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 55.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQuery3() throws InterruptedException {
        log.info("testPatternEvery2 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> e2=Stream2[price>e1.price] -> e3=Stream3[price>e2.price] " +
                "select e1.symbol as symbol1, e2.symbol as symbol2, e3.symbol as symbol3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    Assert.assertArrayEquals(new Object[]{"WSO2", "IBM", "ORACLE"}, inEvents[0].getData());
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 55.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"ORACLE", 65.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        executionPlanRuntime.shutdown();
    }


    @Test
    public void testQuery4() throws InterruptedException {
        log.info("testPatternEvery3 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price1 float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream1[price>20] -> e2=Stream2[price1>e1.price] " +
                "select e1.symbol as symbol1, e2.symbol as symbol2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertArrayEquals(new Object[]{"WSO2", "IBM"}, event.getData());
                                break;
                            case 2:
                                Assert.assertArrayEquals(new Object[]{"GOOG", "IBM"}, event.getData());
                                break;
                            default:
                                Assert.assertSame(2, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 65.6f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 75.7f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"UWO", 80.0f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 2, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQuery5() throws InterruptedException {
        log.info("testPatternEvery4 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every ( e1=Stream1[price>20] -> e3=Stream1[price>20]) -> e2=Stream2[price>e1.price] " +
                "select e1.price as price1, e3.price as price3, e2.price as price2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        Assert.assertArrayEquals(new Object[]{55.6f, 54f, 57.7f}, event.getData());
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 54f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 57.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 1, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQuery6() throws InterruptedException {
        log.info("testPatternEvery5 - OUT 2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every ( e1=Stream1[price>20] -> e3=Stream3[price>20]) -> e2=Stream2[price>e1.price] " +
                "select e1.price as price1, e3.price as price3, e2.price as price2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertArrayEquals(new Object[]{55.6f, 54f, 57.7f}, event.getData());
                                break;
                            case 2:
                                Assert.assertArrayEquals(new Object[]{53.6f, 53f, 57.7f}, event.getData());
                                break;
                            default:
                                Assert.assertSame(2, inEventCount);
                        }

                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOG", 54f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"WSO2", 53.6f, 100});
        Thread.sleep(100);
        stream3.send(new Object[]{"GOOG", 53f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 57.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 2, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQuery7() throws InterruptedException {
        log.info("testPatternEvery5 - OUT 2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every ( e1=Stream1[price>20] -> e3=Stream1[price>20]) -> e2=Stream2[price>e1.price] " +
                "select e1.price as price1, e3.price as price3, e2.price as price2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertArrayEquals(new Object[]{55.6f, 54f, 57.7f}, event.getData());
                                break;
                            case 2:
                                Assert.assertArrayEquals(new Object[]{53.6f, 53f, 57.7f}, event.getData());
                                break;
                            default:
                                Assert.assertSame(2, inEventCount);
                        }

                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");

        executionPlanRuntime.start();

        stream1.send(new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 54f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"WSO2", 53.6f, 100});
        Thread.sleep(100);
        stream1.send(new Object[]{"GOOG", 53f, 100});
        Thread.sleep(100);
        stream2.send(new Object[]{"IBM", 57.7f, 100});
        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 2, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQuery8() throws InterruptedException {
        log.info("testPatternEvery3 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every (e1=Stream[price > 20] -> e2=Stream[symbol == e1.symbol]) " +
                "select e1.volume as volume1, e2.volume as volume2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertArrayEquals(new Object[]{100, 200}, event.getData());
                                break;
                            case 2:
                                Assert.assertArrayEquals(new Object[]{300, 400}, event.getData());
                                break;
                            case 3:
                                Assert.assertArrayEquals(new Object[]{500, 600}, event.getData());
                                break;
                            case 4:
                                Assert.assertArrayEquals(new Object[]{700, 800}, event.getData());
                                break;
                            default:
                                Assert.assertSame(10, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream = executionPlanRuntime.getInputHandler("Stream");

        executionPlanRuntime.start();

        stream.send(new Object[]{"IBM", 75.6f, 100});
        stream.send(new Object[]{"IBM", 75.6f, 200});
        stream.send(new Object[]{"IBM", 75.6f, 300});
        stream.send(new Object[]{"GOOG", 21f, 91});
        stream.send(new Object[]{"IBM", 75.6f, 400});
        stream.send(new Object[]{"IBM", 75.6f, 500});

        stream.send(new Object[]{"GOOG", 21f, 92});

        stream.send(new Object[]{"IBM", 75.6f, 600});
        stream.send(new Object[]{"IBM", 75.6f, 700});
        stream.send(new Object[]{"IBM", 75.6f, 800});
        stream.send(new Object[]{"GOOG", 21f, 93});
        stream.send(new Object[]{"IBM", 75.6f, 900});

        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 4, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQuery9() throws InterruptedException {
        log.info("testPatternEvery3 - OUT 1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream[price > 20] -> e2=Stream[symbol == e1.symbol] " +
                "select e1.volume as volume1, e2.volume as volume2 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertArrayEquals(new Object[]{100, 200}, event.getData());
                                break;
                            case 2:
                                Assert.assertArrayEquals(new Object[]{200, 300}, event.getData());
                                break;
                            case 3:
                                Assert.assertArrayEquals(new Object[]{300, 400}, event.getData());
                                break;
                            case 4:
                                Assert.assertArrayEquals(new Object[]{400, 500}, event.getData());
                                break;
                            case 5:
                                Assert.assertArrayEquals(new Object[]{91, 92}, event.getData());
                                break;
                            case 6:
                                Assert.assertArrayEquals(new Object[]{500, 600}, event.getData());
                                break;
                            case 7:
                                Assert.assertArrayEquals(new Object[]{600, 700}, event.getData());
                                break;
                            case 8:
                                Assert.assertArrayEquals(new Object[]{700, 800}, event.getData());
                                break;
                            case 9:
                                Assert.assertArrayEquals(new Object[]{92, 93}, event.getData());
                                break;
                            case 10:
                                Assert.assertArrayEquals(new Object[]{800, 900}, event.getData());
                                break;
                            default:
                                Assert.assertSame(10, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stream = executionPlanRuntime.getInputHandler("Stream");

        executionPlanRuntime.start();

        stream.send(new Object[]{"IBM", 75.6f, 100});
        stream.send(new Object[]{"IBM", 75.6f, 200});
        stream.send(new Object[]{"IBM", 75.6f, 300});
        stream.send(new Object[]{"GOOG", 21f, 91});
        stream.send(new Object[]{"IBM", 75.6f, 400});
        stream.send(new Object[]{"IBM", 75.6f, 500});

        stream.send(new Object[]{"GOOG", 21f, 92});

        stream.send(new Object[]{"IBM", 75.6f, 600});
        stream.send(new Object[]{"IBM", 75.6f, 700});
        stream.send(new Object[]{"IBM", 75.6f, 800});
        stream.send(new Object[]{"GOOG", 21f, 93});
        stream.send(new Object[]{"IBM", 75.6f, 900});

        Thread.sleep(100);

        Assert.assertEquals("Number of success events", 10, inEventCount);
        Assert.assertEquals("Number of remove events", 0, removeEventCount);
        Assert.assertEquals("Event arrived", true, eventArrived);

        executionPlanRuntime.shutdown();
    }

//    @Test
//    public void testQuery8() throws InterruptedException {
//        log.info("testPatternEvery3 - OUT 1");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//
//        String streams = "" +
//                "define stream Stream1 (symbol string, price float, volume int); " +
//                "define stream Stream1 (symbol string, price float, volume int); " +
//                "define stream Stream3 (symbol string, price float, volume int); ";
//        String query = "" +
//                "@info(name = 'query1') " +
//                "from every e1=Stream1[price>20] -> e2=Stream2[price>e1.price] -> e3=Stream3[price>e2.price] " +
//                "select e1.symbol as symbol1, e2.symbol as symbol2, e3.symbol as symbol3 " +
//                "insert into OutputStream ;";
//
//        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
//
//        executionPlanRuntime.addCallback("query1", new QueryCallback() {
//            @Override
//            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
//                if (inEvents != null) {
//                    for (Event event : inEvents) {
//                        inEventCount++;
////                        switch (inEventCount) {
////                            case 1:
////                                Assert.assertArrayEquals(new Object[]{"WSO2", "IBM"}, event.getData());
////                                break;
////                            case 2:
////                                Assert.assertArrayEquals(new Object[]{"GOOG", "IBM"}, event.getData());
////                                break;
////                            default:
////                                Assert.assertSame(2, inEventCount);
////                        }
//                    }
//                    eventArrived = true;
//                }
//                if (removeEvents != null) {
//                    removeEventCount = removeEventCount + removeEvents.length;
//                }
//                eventArrived = true;
//            }
//
//        });
//
//        InputHandler stream1 = executionPlanRuntime.getInputHandler("Stream1");
//        InputHandler stream2 = executionPlanRuntime.getInputHandler("Stream2");
//        InputHandler stream3 = executionPlanRuntime.getInputHandler("Stream3");
//
//        executionPlanRuntime.start();
//
//        stream1.send(new Object[]{"WSO2", 55.6f, 100});
//        Thread.sleep(100);
//        stream1.send(new Object[]{"GOOG", 65.6f, 100});
//        Thread.sleep(100);
//        stream2.send(new Object[]{"IBM", 75.7f, 100});
//        Thread.sleep(100);
//
//        Assert.assertEquals("Number of success events", 2, inEventCount);
//        Assert.assertEquals("Number of remove events", 0, removeEventCount);
//        Assert.assertEquals("Event arrived", true, eventArrived);
//
//        executionPlanRuntime.shutdown();
//    }
}
