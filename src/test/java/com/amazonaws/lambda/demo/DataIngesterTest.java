package com.amazonaws.lambda.demo;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.core.Index;
import junit.framework.Assert;

@RunWith(MockitoJUnitRunner.class)
public class DataIngesterTest {
    
    private DataIngesterHandler dataIngesterHandler;
    
    @Mock
    private AmazonS3 s3;
    
    @Mock
    private JestClientFactory factory;
    
    @Mock
    private CsvMapper csvMapper;
    
    @Before
    public void init() {
        dataIngesterHandler = new DataIngesterHandler(s3, factory, csvMapper);
    }
    
    @Test
    public void testWhenContextIsNull() {
        String result = dataIngesterHandler.handleRequest(Mockito.mock(S3Event.class), null);
        Assert.assertEquals("Fail: Error in event", result);
    }
    
    @Test
    public void testWhenEventIsNull() {
        String result = dataIngesterHandler.handleRequest(null, Mockito.mock(Context.class));
        Assert.assertEquals("Fail: Error in event", result);
    }
    
    @Test
    public void testWhenClientIsNull() throws IOException {
        String str = IOUtils.toString(this.getClass().getResourceAsStream("/s3-event.put.json"), "UTF-8");
        S3Event event = new S3Event(S3EventNotification.parseJson(str).getRecords()) ;
        Context context = Mockito.mock(Context.class);
        LambdaLogger lambdaLogger = mock(LambdaLogger.class);
        when(context.getLogger()).thenReturn(lambdaLogger);
        doNothing().when(lambdaLogger).log(anyObject());
        JestClient client = mock(JestClient.class);
        when(factory.getObject()).thenReturn(null);
        String result = dataIngesterHandler.handleRequest(event, context);
        Assert.assertEquals("Fail: Cannot connect to the URL provided", result);
    }
    
    @Test
    public void testWhenS3ResponseIsNull() throws IOException {
        String str = IOUtils.toString(this.getClass().getResourceAsStream("/s3-event.put.json"), "UTF-8");
        S3Event event = new S3Event(S3EventNotification.parseJson(str).getRecords()) ;
        Context context = Mockito.mock(Context.class);
        LambdaLogger lambdaLogger = mock(LambdaLogger.class);
        when(context.getLogger()).thenReturn(lambdaLogger);
        doNothing().when(lambdaLogger).log(anyObject());
        JestClient client = mock(JestClient.class);
        when(factory.getObject()).thenReturn(client);
        S3Object response = mock(S3Object.class);
        when(s3.getObject(anyObject())).thenReturn(null);
        String result = dataIngesterHandler.handleRequest(event, context);
        Assert.assertEquals("Fail: Cannot get the S3 object", result);
    }
    
    @Test
    public void testWhenExecuteIsCalledTwice() throws IOException {
        String str = IOUtils.toString(this.getClass().getResourceAsStream("/s3-event.put.json"), "UTF-8");
        S3Event event = new S3Event(S3EventNotification.parseJson(str).getRecords()) ;
        Context context = Mockito.mock(Context.class);
        LambdaLogger lambdaLogger = mock(LambdaLogger.class);
        when(context.getLogger()).thenReturn(lambdaLogger);
        doNothing().when(lambdaLogger).log(anyObject());
        JestClient client = mock(JestClient.class);
        when(factory.getObject()).thenReturn(client);
        S3Object response = mock(S3Object.class);
        when(s3.getObject(anyObject())).thenReturn(response);
        S3ObjectInputStream inputStream = mock(S3ObjectInputStream.class);
        when(response.getObjectContent()).thenReturn(inputStream);
        List<Object> objects = new ArrayList<>();
        objects.add("ABC");
        objects.add("XYZ");
        ObjectReader objectReader = mock(ObjectReader.class);
        when(csvMapper.readerFor(Map.class)).thenReturn(objectReader);
        when(objectReader.with(any(FormatSchema.class))).thenReturn(objectReader);
        MappingIterator<Object> mappingIterator = mock(MappingIterator.class);
        when(objectReader.readValues(any(S3ObjectInputStream.class))).thenReturn(mappingIterator);
        when(mappingIterator.readAll()).thenReturn(objects);
        //when(csvMapper.readerFor(Map.class).with(any(FormatSchema.class)).readValues(any(S3ObjectInputStream.class)).readAll()).thenReturn(objects);
        String result = dataIngesterHandler.handleRequest(event, context);
        verify(client, times(2)).execute(any(Index.class));
        Assert.assertEquals("success", result);
    }


}
