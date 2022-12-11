package diarsid.messaging.filesystem.impl;

import diarsid.filesystem.api.Directory;
import diarsid.messaging.api.Messaging;
import diarsid.messaging.api.exceptions.DestinationNotCreatedException;

import static diarsid.messaging.api.Messaging.Destination.Type.QUEUE;

public class DirectoryForQueue<T> extends DirectoryForDestination<T> implements Messaging.Queue<T> {

    private static final String QUEUE_RECEIVED_MARKERS_DIRECTORY_NAME = ".received";

    public final Directory receivedMarkersDirectory;

    public DirectoryForQueue(MessagingThroughFilesImpl messaging, String name, Serialization<T> serialization, Cleaning cleaning) {
        super(messaging, QUEUE, name, serialization, cleaning);
        this.receivedMarkersDirectory = super
                .directory
                .directoryCreateIfNotExists(QUEUE_RECEIVED_MARKERS_DIRECTORY_NAME)
                .orThrow(() -> new DestinationNotCreatedException(name, QUEUE));
    }

    @Override
    public Messaging.Agent.OfQueue<T> connect(String agentName, Messaging.Receiver<T> receiver) {
        AgentOfQueueDirectory<T> agentOfQueue = new AgentOfQueueDirectory<>(super.messaging, agentName, this, receiver);
        super.messaging.add(agentOfQueue);
        return agentOfQueue;
    }
}
