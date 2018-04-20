package fit.fraunhofer.de.processors.linksmart;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ServiceRegister {

    private String url;
    private String id;
    private String body;
    private boolean usePost; // Whether to use POST or PUT to create entry


    public ServiceRegister(String url, String id, String body) {
        this.url = url;
        this.body = body;

        if (id == null || id.equals("")) {
            usePost = true;
        } else {
            this.id = id;
            usePost = false;
        }
    }

    public void registerService() throws IOException {

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

            HttpUriRequest request;
            InputStreamEntity reqEntity = new InputStreamEntity(
                    new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)), -1, ContentType.APPLICATION_JSON);

            if (usePost) {
                HttpPost post = new HttpPost(url);
                post.setEntity(reqEntity);
                request = post;
            } else {
                HttpPut put = new HttpPut(url + "/" + id);
                put.setEntity(reqEntity);
                request = put;
            }

            ResponseHandler<ServiceEntry> responseHandler = response -> {
                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    HttpEntity entity = response.getEntity();
                    Gson gson = new Gson();
                    String s = entity != null ? EntityUtils.toString(entity) : null;
                    return gson.fromJson(s, ServiceEntry.class);
                } else {
                    throw new ClientProtocolException("Unexpected response status: " + status);
                }
            };

            ServiceEntry serviceEntry = httpClient.execute(request, responseHandler);
            id = serviceEntry.getId();
            usePost = false;

        }


    }

    public void deregisterService() throws IOException {

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            String deleteUrl = url + "/" + id;
            HttpDelete httpDelete = new HttpDelete(deleteUrl);
            ResponseHandler<String> responseHandler = response -> {
                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    return null;
                } else {
                    HttpEntity entity = response.getEntity();
                    Gson gson = new Gson();
                    String s = entity != null ? EntityUtils.toString(entity) : null;
                    ErrorResponse er = gson.fromJson(s, ErrorResponse.class);
                    throw new ClientProtocolException("Error response code: " + er.getCode() + ", reason: " + er.getMessage());
                }
            };
            httpClient.execute(httpDelete, responseHandler);

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
