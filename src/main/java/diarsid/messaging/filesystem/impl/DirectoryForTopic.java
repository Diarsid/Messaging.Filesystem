package diarsid.messaging.filesystem.impl;

import diarsid.messaging.api.Messaging;

import static diarsid.messaging.api.Messaging.Destination.Type.TOPIC;

public class DirectoryForTopic<T> extends DirectoryForDestination<T> implements Messaging.Topic<T> {

    public DirectoryForTopic(MessagingThroughFilesImpl messaging, String name, Serialization<T> serialization, Cleaning cleaning) {
        super(messaging, TOPIC, name, serialization, cleaning);
    }

    @Override
    public Messaging.Agent.OfTopic<T> connect(String agentName, Messaging.Receiver<T> receiver) {
        AgentOfTopicDirectory<T> agentOfTopic = new AgentOfTopicDirectory<>(super.messaging, agentName, this, receiver);
        super.messaging.add(agentOfTopic);
        return agentOfTopic;
    }
}
