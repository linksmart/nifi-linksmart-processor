/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fit.fraunhofer.de.processors.linksmart;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;



public class LinksmartGatewayTest {

    private TestRunner testRunner;


    @Before
    public void init() {

        testRunner = TestRunners.newTestRunner(LinksmartGateway.class);
    }


    @Test
    public void testPrintOneMsgWithBatchSizeOne() {
        testRunner.setProperty("COMMAND_LINE", "python " + getResourceFilePath("print_one_msg_and_wait.py"));
        testRunner.setProperty("MAX_BATCH_SIZE", "1");
        testRunner.run(1, false, true);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        testRunner.run(1, true, false);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(LinksmartGateway.REL_SUCCESS);

        assertEquals("One flow file should be in REL_SUCCESS", 1, results.size());
    }

    @Test
    public void testPrintMultipleMsgWithLargerBatchSize() {
        testRunner.setProperty("COMMAND_LINE", "python " + getResourceFilePath("print_multiple_msg_and_wait.py"));
        testRunner.setProperty("MAX_BATCH_SIZE", "5");
        testRunner.run(1, false, true);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        testRunner.run(1, true, false);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(LinksmartGateway.REL_SUCCESS);

        assertEquals("No flow file should be in REL_SUCCESS", 0, results.size());

    }

    @Test
    public void testPrintMultipleMsgWithExactBatchSize() {
        testRunner.setProperty("COMMAND_LINE", "python " + getResourceFilePath("print_multiple_msg_and_wait.py"));
        testRunner.setProperty("MAX_BATCH_SIZE", "3");
        testRunner.run(1, false, true);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        testRunner.run(1, true, false);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(LinksmartGateway.REL_SUCCESS);

        assertEquals("No flow file should be in REL_SUCCESS", 1, results.size());
    }

    @Test
    public void testPrintMultipleMsgWithBatchSizeOne() {
        testRunner.setProperty("COMMAND_LINE", "python " + getResourceFilePath("print_multiple_msg_and_wait.py"));
        testRunner.setProperty("MAX_BATCH_SIZE", "1");
        testRunner.run(1, false, true);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        testRunner.run(1, true, false);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(LinksmartGateway.REL_SUCCESS);

        assertEquals("No flow file should be in REL_SUCCESS", 3, results.size());

    }

    @Test
    public void testSpecialDelimiter() {
        testRunner.setProperty("COMMAND_LINE", "python " + getResourceFilePath("print_multiple_msg_and_wait.py"));
        testRunner.setProperty("MAX_BATCH_SIZE", "3");
        testRunner.setProperty("MESSAGE_DELIMITER", "[s]");
        testRunner.run(1, false, true);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        testRunner.run(1, true, false);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(LinksmartGateway.REL_SUCCESS);

        assertEquals("No flow file should be in REL_SUCCESS", 1, results.size());

        String flowFileContent = new String(testRunner.getContentAsByteArray(results.get(0)));

        assertEquals("Flowfile content not the same as expected. ",
                "Msg 0 printed by Python[s]Msg 1 printed by Python[s]Msg 2 printed by Python",
                flowFileContent);
    }


    private String getResourceFilePath(String fileName) {

        String relPath = "src\\test\\resources\\" + fileName;
        return (new File(relPath)).getAbsolutePath();

    }

}
