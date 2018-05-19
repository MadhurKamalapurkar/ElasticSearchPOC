package com.amazonaws.lambda.demo;

import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseFactory;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.message.BasicStatusLine;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SearchHandlerTest {
    
    private SearchHandler searchHandler;
    
    @Mock
    private JSONParser parser;
    
    @Mock
    private HttpClient httpClient;
    
    @Before
    public void init() {
        searchHandler = new SearchHandler(parser, httpClient);
    }
    
    @Test
    public void testCallEs() throws ParseException, IOException {
        HttpResponseFactory factory = new DefaultHttpResponseFactory();
        HttpResponse response = factory.newHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, null), null);
        response.setEntity(new StringEntity("entity"));
        when(httpClient.execute(Mockito.any(HttpPost.class))).thenReturn(response);
        JSONObject jsonObject = searchHandler.callES("something");
        Assert.assertNotNull(jsonObject);
        Assert.assertEquals("something", jsonObject.get("body"));
        Assert.assertEquals("200", jsonObject.get("statusCode"));
    }

}
