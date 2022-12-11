package diarsid.messaging.filesystem;

import diarsid.messaging.api.Messaging;
import diarsid.messaging.filesystem.impl.MessagingThroughFilesImpl;

import static diarsid.support.concurrency.test.CurrentThread.async;

public class DemoWithQueue {

    private final static Messaging.Receiver<String> RECEIVER = (offset, s) -> {
        System.out.println("[RECEIVED] offset:" + offset + " message:" + s);
    };

    public static class SenderAndAgent1 {

        public static void main(String[] args) {
            Messaging messaging = new MessagingThroughFilesImpl("TEST");

            Messaging.Queue<String> queue = messaging.queue("queue_1", String.class);

//            Messaging.Agent.OfQueue<String> agent1 = queue.connect("agent_Q1", RECEIVER);
//            agent1.startWork();

            async()
                    .loopEndless()
                    .eachTimeSleep(1000)
                    .eachTimeDo((i) -> {
                        queue.send("message_" + i);
                    });
        }
    }

    public static class AgentA {

        public static void main(String[] args) {
            Messaging messaging = new MessagingThroughFilesImpl("TEST");

            Messaging.Queue<String> queue = messaging.queue("queue_1", String.class);

            Messaging.Agent.OfQueue<String> agent1 = queue.connect("agent_Qa", RECEIVER);
            agent1.startWork();
        }
    }

    public static class AgentA_instance1 {

        public static void main(String[] args) {
            Messaging messaging = new MessagingThroughFilesImpl("TEST");

            Messaging.Queue<String> queue = messaging.queue("queue_1", String.class);

            Messaging.Agent.OfQueue<String> agent1 = queue.connect("agent_Qa", RECEIVER);
            agent1.startWork();
        }
    }
}
