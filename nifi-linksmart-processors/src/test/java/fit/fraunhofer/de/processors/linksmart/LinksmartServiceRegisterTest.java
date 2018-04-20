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
import org.apache.http.HttpEntity;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


public class LinksmartServiceRegisterTest {

    private TestRunner testRunner;
    private String url;
    private String body;


    @Before
    public void init() {

        testRunner = TestRunners.newTestRunner(LinksmartServiceRegister.class);

        url = "http://iot-linksmart.iot:8082/";
        body = "{" +
                "\"description\": \"Nifi Test\"," +
                "\"name\": \"_nifi._tcp\"," +
                "\"apis\": {" +
                "}," +
                "\"docs\": [" +
                "{" +
                "\"description\": \"Documentation\"," +
                "\"type\": \"text/html\"," +
                "\"url\": \"http://doc.linksmart.eu/DGW\"," +
                "\"apis\": null" +
                "}" +
                "]," +
                "\"meta\": {" +
                "\"api_version\": \"1.1.0\"," +
                "\"ls_codename\": \"DGW\"" +
                "}," +
                "\"ttl\": 120" +
                "}";
    }

    @Test
    public void testFlowFileContent() {
        testRunner.setProperty(LinksmartServiceRegister.SC_URL, url);
        testRunner.setProperty(LinksmartServiceRegister.ID, "nifi_test");
        testRunner.setProperty(LinksmartServiceRegister.BODY, body);
        testRunner.enqueue("Dummy flow file");
        testRunner.run(1, false, true);

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(LinksmartServiceRegister.REL_SUCCESS);
        assertEquals("One flow file should be in REL_SUCCESS", 1, results.size());
        assertEquals("Dummy flow file", new String(testRunner.getContentAsByteArray(results.get(0))));
    }

    @Test
    public void testRegisterUsingBodyWithId() throws IOException {
        testRunner.setProperty(LinksmartServiceRegister.SC_URL, url);
        testRunner.setProperty(LinksmartServiceRegister.ID, "nifi_test");
        testRunner.setProperty(LinksmartServiceRegister.BODY, body);
        testRunner.run(1, false, true);

        assertTrue(checkServiceExistence("nifi_test"));

    }

    @Test
    public void testRegisterUsingBodyWithoutId() throws IOException {
        // TODO: test body without ID
        testRunner.setProperty(LinksmartServiceRegister.SC_URL, url);
        testRunner.setProperty(LinksmartServiceRegister.ID, "");
        testRunner.setProperty(LinksmartServiceRegister.BODY, body);
        testRunner.run(1, false, true);

        assertTrue(checkServiceExistenceWithoutId("_nifi._tcp"));
    }

    @Test
    public void testDeregister() throws IOException {
        testRunner.setProperty(LinksmartServiceRegister.SC_URL, url);
        testRunner.setProperty(LinksmartServiceRegister.ID, "nifi_test");
        testRunner.setProperty(LinksmartServiceRegister.BODY, body);
        testRunner.run(1);

        assertFalse(checkServiceExistence("nifi_test"));

    }


    private boolean checkServiceExistence(String id) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String getUrl = url + "/" + id;
            HttpGet httpGet = new HttpGet(getUrl);
            ResponseHandler<Boolean> responseHandler = response -> {
                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    return true;
                } else {
                    HttpEntity entity = response.getEntity();
                    Gson gson = new Gson();
                    String s = entity != null ? EntityUtils.toString(entity) : null;
                    ErrorResponse er = gson.fromJson(s, ErrorResponse.class);
                    System.out.println("Error response code: " + er.getCode() + ", reason: " + er.getMessage());
                    return false;
                }
            };
            return httpClient.execute(httpGet, responseHandler);
        }
    }

    private boolean checkServiceExistenceWithoutId(String name) throws IOException {

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String getUrl = url;
            HttpGet httpGet = new HttpGet(getUrl);
            ResponseHandler<Boolean> responseHandler = response -> {
                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    HttpEntity entity = response.getEntity();
                    Gson gson = new Gson();
                    String s = entity != null ? EntityUtils.toString(entity) : null;
                    ServiceList l = gson.fromJson(s, ServiceList.class);

                    for (ServiceEntry serviceEntry : l.getServices()) {
                        if (serviceEntry.getName().equals(name)) {
                            return true;
                        }
                    }
                    return false;
                } else {
                    HttpEntity entity = response.getEntity();
                    Gson gson = new Gson();
                    String s = entity != null ? EntityUtils.toString(entity) : null;
                    ErrorResponse er = gson.fromJson(s, ErrorResponse.class);
                    System.out.println("Error response code: " + er.getCode() + ", reason: " + er.getMessage());
                    return false;
                }
            };

            return httpClient.execute(httpGet, responseHandler);
        }
    }

    private class ServiceList {
        private String id;
        private String description;
        private List<ServiceEntry> services;

        public List<ServiceEntry> getServices() {
            return services;
        }
    }

    private class ServiceEntry {
        private String id;
        private String name;
        private String description;
        private Map<String, String> apis;
        private Map<String, String> meta;
        private int ttl;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public Map<String, String> getApis() {
            return apis;
        }

        public void setApis(Map<String, String> apis) {
            this.apis = apis;
        }

        public Map<String, String> getMeta() {
            return meta;
        }

        public void setMeta(Map<String, String> meta) {
            this.meta = meta;
        }

        public int getTtl() {
            return ttl;
        }

        public void setTtl(int ttl) {
            this.ttl = ttl;
        }

        public String toString() {
            return "id: " + id + ", " + name + ", " + description;
        }
    }

    private class ErrorResponse {
        private int code;
        private String message;

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
