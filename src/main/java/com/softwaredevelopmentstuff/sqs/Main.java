package com.softwaredevelopmentstuff.sqs;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.lang.System.out;

/**
 * Created by mariusrop on 21.02.2017.
 * <p>
 * It starts a couple of producers that post some messages on a queue indeffinetly.
 * It also starts a couple of consumers that read the messages and delete them.
 */
public class Main {

    private static final String QUEUE_URL = "https://sqs.eu-west-1.amazonaws.com/075601680455/mariusSqs";
    private static final AmazonSQSClient sqsClient = new AmazonSQSClient(new ProfileCredentialsProvider("training"));
    private static final AtomicInteger noReceived = new AtomicInteger(0);
    private static final AtomicInteger noSent = new AtomicInteger(0);
    private static final ConcurrentHashMap<String, String> consumed = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        ExecutorService producers = Executors.newFixedThreadPool(3);
        IntStream.range(0, 3).forEach(i -> producers.execute(Main::produceMessages));

        ExecutorService consumers = Executors.newFixedThreadPool(10);
        IntStream.range(0, 10).forEach(i -> consumers.execute(Main::consumeMessages));
    }

    private static void produceMessages() {
        String producerName = "producer" + Thread.currentThread().getId();
        out.println(producerName + ": started");

        while (true) {
            SendMessageResult result = sqsClient.sendMessage(
                    new SendMessageRequest()
                            .withQueueUrl(QUEUE_URL)
                            .withMessageBody(valueOf(System.currentTimeMillis()))
                            .withDelaySeconds(1));
            out.println(format("%s: sent %s; total sent %d", producerName, result.getMessageId(), noSent.incrementAndGet()));
        }
    }

    private static void consumeMessages() {
        String consumerName = "consumer" + Thread.currentThread().getId();

        out.println(consumerName + ": started");

        while (true) {
            sqsClient.receiveMessage(QUEUE_URL).getMessages().forEach(message -> {
                out.println(format("%s: received %s; total received %d", consumerName, message.getMessageId(), noReceived.incrementAndGet()));
                sqsClient.deleteMessage(QUEUE_URL, message.getReceiptHandle());

                String prev = consumed.putIfAbsent(message.getMessageId(), message.getBody());

                if (prev != null) {
                    out.println(consumerName + ": MESSAGE DELIVERED TWICE!!! " + message.getMessageId());
                }
            });
        }
    }
}
