package main.java.singular.com.sqs;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.google.gson.Gson;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;


public class ProcessSQSEvents implements RequestHandler<SQSEvent, Void> {

    final static Logger logger = Logger.getLogger(ProcessSQSEvents.class);

    static class Requests {
        public List<String> requests = new LinkedList();
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context)
    {
        System.out.println("starting2");
        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {

            for(SQSMessage msg : event.getRecords()) {
                String body = msg.getBody();
                Requests requests = new Gson().fromJson(body, Requests.class);
                for(String request : requests.requests) {
                    try {
                        HttpGet httpRequest = new HttpGet(request);
                        client.execute(httpRequest);
                    }
                    catch (Exception e) {
                        System.out.println("Exception occurred while processing request: " + e.getMessage());
                    }
                }
            }

        } catch (IOException e) {
            //logger.error("Exception occurred", e);
            System.out.println("Exception occurred: " + e.getMessage());

        }

        return null;
    }
}