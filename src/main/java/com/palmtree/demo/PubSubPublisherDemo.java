package com.palmtree.demo;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

public class PubSubPublisherDemo {

    private static final String PROJECT_ID = "testproject";
    private static final String TOPIC_ID = "pubsubDemoTopic";

    public static void main(String... args) throws Exception {
        try {
            publishMessages();
        } catch (Exception e) {
            System.out.println("Failed while publishing pub/sub messages - " + e.getMessage());
        }
    }

    private static void publishMessages() throws Exception {
        Publisher publisher = null;
        try {

            TopicName topic = TopicName.create(PROJECT_ID, TOPIC_ID);
            String message1 = "Hello - test";
            publisher = Publisher.defaultBuilder(topic).build();

            for (int i=0; i < 10; i++) {
                String message = message1 + i;
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                System.out.println("Published message: " + message + " to " + topic + " - " + messageIdFuture);
            }
        } catch (Exception e) {
            System.out.println("Failed while publishing message Topic - " + e.getMessage());
        } finally {
            if (publisher != null) {
                publisher.shutdown();
            }
        }
    }
}
