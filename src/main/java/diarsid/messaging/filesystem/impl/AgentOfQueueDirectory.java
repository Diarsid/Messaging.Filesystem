package diarsid.messaging.filesystem.impl;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diarsid.messaging.api.Messaging;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import static diarsid.messaging.filesystem.impl.AgentOfDirectory.MessageLockAttemptResult.MESSAGE_LOCK_ACQUIRED;
import static diarsid.messaging.filesystem.impl.AgentOfDirectory.MessageLockAttemptResult.MESSAGE_LOCK_ACQUIRED_BY_ANOTHER_AGENT;
import static diarsid.messaging.filesystem.impl.AgentOfDirectory.MessageLockAttemptResult.MESSAGE_LOCK_CANNOT_BE_ACQUIRED;

public class AgentOfQueueDirectory<T> extends AgentOfDirectory<T> implements Messaging.Agent.OfQueue<T>  {

    private static final Logger log = LoggerFactory.getLogger(AgentOfQueueDirectory.class);

    public AgentOfQueueDirectory(
            MessagingThroughFilesImpl messaging,
            String name,
            DirectoryForQueue<T> queueDirectory,
            Messaging.Receiver<T> receiver) {
        super(messaging, name, queueDirectory, receiver);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Messaging.Queue<T> queue() {
        return (Messaging.Queue<T>) super.destination;
    }

    @Override
    MessageLockAttemptResult tryLockMessage(MessagePath messagePath) {
        Path receivedMarker = ((DirectoryForQueue<T>) this.destination)
                .receivedMarkersDirectory
                .path()
                .resolve(messagePath.offsetString);

        try (var markerChannel = FileChannel.open(receivedMarker, READ, WRITE, CREATE_NEW)) {
            // lock acquired means THIS instance can process message
            return MESSAGE_LOCK_ACQUIRED;
        }
        catch (FileAlreadyExistsException expected) {
            return MESSAGE_LOCK_ACQUIRED_BY_ANOTHER_AGENT;
        }
        catch (IOException e) {
            log.error("Exception during received marker creation: ", e);
            return MESSAGE_LOCK_CANNOT_BE_ACQUIRED;
        }
    }
}
