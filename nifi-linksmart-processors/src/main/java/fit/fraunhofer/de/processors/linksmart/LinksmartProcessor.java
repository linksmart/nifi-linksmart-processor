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

import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessBuilder;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Tags({"linksmart"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("This is a Nifi processor which serves similar as a LinkSmart gateway.")
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
@TriggerSerially  // TODO: consider whether trigger serially could help
public class LinksmartProcessor extends AbstractProcessor {

    public static final PropertyDescriptor COMMAND_LINE = new PropertyDescriptor
            .Builder().name("COMMAND_LINE")
            .displayName("Command Line")
            .description("The command line to be executed in a subprocess.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("MAX_MESSAGE_QUEUE_SIZE")
            .displayName("Maximum Message Queue Size")
            .description("The maximum size of the internal queue used to buffer messages being transferred from the underlying channel to the processor. " +
                    "Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total " +
                    "memory used by the processor.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("MAX_BATCH_SIZE")
            .displayName("Maximum Batch Size")
            .description(
                    "The maximum number of messages to add to a single FlowFile. If multiple messages are available, they will be concatenated along with "
                            + "the <Message Delimiter> up to this configured maximum number of messages")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .defaultValue("1")
            .required(true)
            .build();

    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
            .name("MESSAGE_DELIMITER")
            .displayName("Batching Message Delimiter")
            .description("Specifies the delimiter to place between messages when multiple messages are bundled together (see <Max Batch Size> property).")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("\\n")
            .required(true)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages received successfully will be sent out this relationship.")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private BlockingQueue<String> stdoutQueue;
    private BlockingQueue<String> stderrQueue;
    private NuProcess process;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(COMMAND_LINE);
        descriptors.add(MAX_MESSAGE_QUEUE_SIZE);
        descriptors.add(MAX_BATCH_SIZE);
        descriptors.add(MESSAGE_DELIMITER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);


    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception {

        System.out.println("onSchedule called");

        final String cmd = context.getProperty(COMMAND_LINE).getValue();
        final int queueSize = context.getProperty(MAX_MESSAGE_QUEUE_SIZE).asInteger();

        stdoutQueue = new LinkedBlockingQueue<>(queueSize);
        stderrQueue = new LinkedBlockingQueue<>(20);

        String[] cmdArray = cmd.trim().split("\\s+");
        NuProcessBuilder pb = new NuProcessBuilder(cmdArray);
        LinksmartProcessHandler handler = new LinksmartProcessHandler(stdoutQueue, stderrQueue);
        pb.setProcessListener(handler);
        process = pb.start(); // TODO: wrap exception in more readable text

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        System.out.println("onTrigger called");

        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        final String msgDelimiter = context.getProperty(MESSAGE_DELIMITER).getValue()
                .replace("\\n", "\n").replace("\\r", "\r")
                .replace("\\t", "\t");

        byte[] msgDelimiterBytes = msgDelimiter.getBytes(Charset.forName("UTF-8"));

        // Put messages into flowfiles
        while (stdoutQueue.size() >= maxBatchSize) { // Continue to loop, as long as the remaining queue size is not less than the batch size
            FlowFile flowFile = session.create();
            for (int i = 0; i < maxBatchSize; i++) {
                String msg = stdoutQueue.poll();
                System.out.println("onTrigger got msg: " + msg);
                final boolean writeDelimiter = (i > 0);

                try {
                    flowFile = session.append(flowFile, out -> {
                        if (writeDelimiter) {
                            out.write(msgDelimiterBytes);
                        }

                        out.write(msg.getBytes(Charset.forName("UTF-8")));
                    });

                } catch (Exception e) { // TODO: implement recovery
                    getLogger().error("Failed to write contents of the message to FlowFile due to {}.",
                            new Object[]{e.getMessage()}, e);
                    break;
                }
            }
            session.transfer(flowFile, REL_SUCCESS);
        }

        // In case stderr not empty, gather the exception message
        String errMsg = "";
        while (stderrQueue.size() > 0) {
            errMsg = errMsg + stderrQueue.poll();
        }

        // check if process still running
        if (process != null) {
            if (!process.isRunning()) {
                errMsg = errMsg + "The sub-process has stopped!";
            }
        }

        if (!"".equals(errMsg)) {
            // Throw RuntimeException to let the Nifi framework handle it
            throw new RuntimeException("Error while running sub-process: " + errMsg);
        }

        // TODO: consider whether to yield
        context.yield();

    }


    @OnStopped
    public void onStopped(final ProcessContext context) throws Exception {
        System.out.println("onStopped called");

        // TODO: consider possibility to simplify the process
        if (process != null && process.isRunning()) {
            getLogger().info("Soft-killing sub-process...");
            process.destroy(false);

            if (process.waitFor(3, TimeUnit.SECONDS) == Integer.MIN_VALUE) { // If timeout is reached
                getLogger().warn("Failed to kill sub-process via soft-killing failed. Killing it by force now. Sub-process may not exit cleanly.");
                process.destroy(true);
            }

        }

    }
}
