package com.amazonaws.lambda.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

public class SearchHandler implements RequestStreamHandler {
    private JSONParser parser = new JSONParser();
    // Removed URL for safety purpose
    private static final String URL = "";

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        JSONObject response = new JSONObject();
        String q = "";
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            JSONObject event = (JSONObject) parser.parse(reader);
            context.getLogger().log("query params: " + event.get("queryStringParameters"));
            if (event.get("queryStringParameters") != null) {
                JSONObject qps = (JSONObject) event.get("queryStringParameters");
                if (qps.get("q") != null) {
                    q = (String) qps.get("q");
                }
            }

            context.getLogger().log("query param: " + q);
            response = callES(q);

        } catch (org.json.simple.parser.ParseException e) {
            response.put("statusCode", 500);
            response.put("body", e.getMessage());
            context.getLogger().log(e.getMessage());
        } catch (Exception e) {
            response.put("statusCode", 500);
            response.put("body", e.getMessage());
            context.getLogger().log(e.getMessage());
        }

        OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");
        writer.write(response.toJSONString());
        writer.close();
    }

    public JSONObject callES(final String query) throws ParseException, IOException {
        HttpClient httpClient = new DefaultHttpClient();
        JSONObject responseJson = new JSONObject();
        String responseString = "";
        HttpPost httpPost = new HttpPost(URL);
        httpPost.setHeader("Content-Type", ContentType.APPLICATION_JSON.toString());
        String body = "{\"query\": {\"multi_match\" : {\"query\":" + "\"" + query + "\""
                + ", \"fields\":[\"PLAN_NAME\", \"SPONSOR_DFE_NAME\", \"SPONS_DFE_MAIL_US_STATE\"],\"operator\": \"or\"}}}";
        StringEntity stringEntity = new StringEntity(body);
        httpPost.setEntity(stringEntity);
        HttpResponse httpResponse = httpClient.execute(httpPost);
        HttpEntity entity = httpResponse.getEntity();
        responseString = EntityUtils.toString(entity, "UTF-8");
        responseJson.put("body", responseString);
        responseJson.put("statusCode", httpResponse.getStatusLine().getStatusCode());
        return responseJson;
    }

}
