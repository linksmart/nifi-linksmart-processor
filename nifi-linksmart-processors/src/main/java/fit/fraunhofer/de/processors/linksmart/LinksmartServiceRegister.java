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

import com.google.gson.Gson;
import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessBuilder;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Tags({"linksmart"})
@CapabilityDescription("This is a Nifi processor which register this Nifi instance to the Linksmart Service Catalog.")
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class LinksmartServiceRegister extends AbstractProcessor {

    public static final PropertyDescriptor SC_URL = new PropertyDescriptor
            .Builder().name("SC_URL")
            .displayName("Service Catalog URL")
            .description("The URL of the Linksmart Service Catalog.")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ID = new PropertyDescriptor
            .Builder().name("ID")
            .displayName("Service ID")
            .description("The ID of this service, which will be used for identification in the Linksmart Service Catalog. If left " +
                    "empty, a random UUID will be assigned automatically by the Service Catalog")
            .addValidator(Validator.VALID)
            .build();

/*    public static final PropertyDescriptor CONTENT_SOURCE = new PropertyDescriptor
            .Builder().name("CONTENT_SOURCE")
            .displayName("Source of Content")
            .description("Choose the source of content. 'JSON' will only use the content specified in 'Service Entry Body'" +
                    "as the message sent to the Service Catalog. Other data fields below will be ignored. If 'Data Field' is " +
                    "chosen, the data specified in the below data fields will be used to formulate the body for the Service Catalog" +
                    "registration message.")
            .allowableValues("JSON", "Data Field")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESCRIPTION = new PropertyDescriptor
            .Builder().name("DESCRIPTION")
            .displayName("Service Description")
            .description("A user friendly description to describe this service.")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor PROPERTY_NAME = new PropertyDescriptor
            .Builder().name("PROPERTY_NAME")
            .displayName("Property Name")
            .description("The property name containing info about exposed APIs.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("TTL")
            .displayName("Time To Live (in seconds)")
            .description("The Time To Live (TTL) of this service entry in the Linksmart Service Catalog.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("120")
            .required(true)
            .build();*/

    public static final PropertyDescriptor BODY = new PropertyDescriptor.Builder()
            .name("BODY")
            .displayName("Service Entry Body")
            .description("The content to be sent to the Service Catalog")
            .defaultValue("")
            .required(true)
            .addValidator(new JsonValidator())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Messages received successfully will be sent out this relationship.")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private ServiceRegister serviceRegister;
    private String serviceUuid;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SC_URL);
        descriptors.add(ID);
        /*descriptors.add(CONTENT_SOURCE);
        descriptors.add(DESCRIPTION);
        descriptors.add(PROPERTY_NAME);
        descriptors.add(TTL);*/
        descriptors.add(BODY);
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
        String url = context.getProperty(SC_URL).getValue();
        String id = context.getProperty(ID).getValue();
        String body = context.getProperty(BODY).getValue();

        serviceRegister = new ServiceRegister(url, id, body);
        serviceRegister.registerService();

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (serviceRegister != null) {
            try {
                serviceRegister.registerService();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // TODO: update service

        FlowFile flowFile = session.get();
        if(flowFile != null) {
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    @OnStopped
    public void onStopped(final ProcessContext context) throws Exception {
        if (serviceRegister != null) {
            serviceRegister.deregisterService();
            serviceRegister = null; // Release memory
        }
    }

    // Validator for Json string
    static private class JsonValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            Gson gson = new Gson();
            boolean isValid;
            try {
                gson.fromJson(value, Object.class);
                isValid = true;
            } catch(com.google.gson.JsonSyntaxException ex) {
                isValid = false;
            }

            return new ValidationResult.Builder().subject(subject).input(value)
                    .valid(isValid).explanation(subject + " must be valid json format").build();

        }
    }

}
