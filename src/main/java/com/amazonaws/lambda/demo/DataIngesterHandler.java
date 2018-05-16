package com.amazonaws.lambda.demo;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

public class DataIngesterHandler implements RequestHandler<S3Event, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	private ObjectMapper mapper = new ObjectMapper();
	private JestClientFactory factory = new JestClientFactory();
	// Removed URL for safety purpose
	private static final String URL = "";

	public DataIngesterHandler() {
	}

	// Test purpose only.
	DataIngesterHandler(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public String handleRequest(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event);

		// Get the object from the event and show its content type
		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();
		factory.setHttpClientConfig(new HttpClientConfig.Builder(URL).multiThreaded(true).build());
		JestClient client = factory.getObject();
		S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
		InputStream input = response.getObjectContent();
		// Read data from CSV file
		List<Object> readAll = new ArrayList<>();
		try {
			CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
			CsvMapper csvMapper = new CsvMapper();
			readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(input).readAll();

			List<Index> indexes = new ArrayList<Index>();
			readAll.parallelStream().forEach(object -> {
				try {
					indexes.add(new Index.Builder(mapper.writeValueAsString(object)).build());
					Index index = new Index.Builder(mapper.writeValueAsString(object)).index("report").type("sales")
							.build();
					client.execute(index);
				} catch (Exception e) {
					e.printStackTrace();
					context.getLogger().log(e.getMessage());
				}

			});

			return "success";
		} catch (Exception e) {
			e.printStackTrace();
			context.getLogger().log(e.getMessage());
			return "Fail: " + e.getMessage();
		} finally {
			if (client != null) {
				try {
					client.close();
				} catch (Exception e) {
					e.printStackTrace();
					context.getLogger().log(e.getMessage());
					return "Fail: " + e.getMessage();
				}

			}
		}
	}
	
	/*
     * Asynchronous code, cannot be included since lambda is not active once the session is killed
    if (client == null) {
    	System.out.println("Client is null");
    	return "Client is null: Fail";
    } else {
    	Bulk bulk = new Bulk.Builder().defaultIndex("report_16").defaultType("sales1").addAction(indexes).build();
    	client.executeAsync(bulk, new JestResultHandler<JestResult>() {

			@Override
			public void completed(JestResult result) {
				System.out.println(result.getJsonString());
			}

			@Override
			public void failed(Exception ex) {
				System.out.println(ex.getMessage());
			}
		});
    }
    */

}