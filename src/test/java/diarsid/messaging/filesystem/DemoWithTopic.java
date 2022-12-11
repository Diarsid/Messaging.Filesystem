package diarsid.messaging.filesystem;

import java.time.Duration;

import diarsid.messaging.api.Messaging;
import diarsid.messaging.filesystem.impl.MessagingThroughFilesImpl;
import diarsid.messaging.filesystem.defaultimpl.cleaning.CleanFilesByRemovingOlderThan;

import static java.util.concurrent.TimeUnit.SECONDS;

import static diarsid.support.concurrency.test.CurrentThread.async;

public class DemoWithTopic {

    private final static Messaging.Receiver<String> RECEIVER = (offset, s) -> {
        System.out.println("[RECEIVED] offset:" + offset + " message:" + s);
    };

    public static class SenderAndAgent1 {

        public static void main(String[] args) {
            Messaging messaging = new MessagingThroughFilesImpl("TEST");

            Messaging.Destination.Cleaning cleaning = new CleanFilesByRemovingOlderThan(10, SECONDS, Duration.ofSeconds(5));

            Messaging.Topic<String> topic = messaging.topic("topic_1", String.class, cleaning);

            Messaging.Agent.OfTopic<String> agent1 = topic.connect("agent_1", RECEIVER);
            agent1.startWork();

            async()
                    .loopEndless()
                    .eachTimeSleep(1000)
                    .eachTimeDo((i) -> {
                        topic.send("message_" + i);
                    });
        }
    }

    public static class AgentA {

        public static void main(String[] args) {
            Messaging messaging = new MessagingThroughFilesImpl("TEST");

            Messaging.Topic<String> topic = messaging.topic("topic_1", String.class);

            Messaging.Agent.OfTopic<String> agent1 = topic.connect("agent_a", RECEIVER);
            agent1.startWork();
        }
    }

    public static class AgentA_instance1 {

        public static void main(String[] args) {
            Messaging messaging = new MessagingThroughFilesImpl("TEST");

            Messaging.Topic<String> topic = messaging.topic("topic_1", String.class);

            Messaging.Agent.OfTopic<String> agent1 = topic.connect("agent_a", RECEIVER);
            agent1.startWork();
        }
    }
}
