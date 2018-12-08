package main.java.singular.com.sqs;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.google.gson.Gson;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import io.sentry.Sentry;
import io.sentry.SentryClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;


public class ProcessSQSEvents implements RequestHandler<SQSEvent, Void> {

    //final static Logger logger = Logger.getLogger(ProcessSQSEvents.class);

    private static final StatsDClient statsd = new NonBlockingStatsDClient(
            "mparticle.sqs.",                          /* prefix to any stats; may be null or empty string */
            System.getenv("DATADOG_HOSTNAME"),                        /* common case: localhost */
            8125,                                 /* port */
            new String[] {"tag:value"}            /* Datadog extension: Constant tags, always applied */
    );

    final static String SQSQueueURL = System.getenv("SQS_QUEUE_URL");

    static class Requests {
        public List<String> requests = new LinkedList();
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context)
    {
        Sentry.init();

        try (CloseableHttpAsyncClient client = HttpAsyncClients.createDefault()) {

            List<SQSEvent.SQSMessage> records = event.getRecords();
            for(SQSMessage msg : records) {
                String body = msg.getBody();
                Requests requests = new Gson().fromJson(body, Requests.class);
                long start = System.currentTimeMillis();
                for(String request : requests.requests) {
                    try {
                        HttpGet httpRequest = new HttpGet(request);
                        Future<HttpResponse> responseF = client.execute(httpRequest, null);
                    }
                    catch (Exception e) {
                        System.out.println("Exception occurred while processing request: " + e.getMessage());
                    }
                }
                long runTime = System.currentTimeMillis() - start;
                statsd.histogram("batch.execution.time", runTime);
                statsd.histogram("batch.size", requests.requests.size());
            }
            statsd.histogram("lambda.batch.size", records.size());

        } catch (IOException e) {
            statsd.incrementCounter("unexpected.errors");
            Sentry.capture(e);
            //logger.error("Exception occurred", e);
            System.out.println("Exception occurred: " + e.getMessage());
        }

        return null;
    }

}