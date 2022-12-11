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

public class AgentOfTopicDirectory<T> extends AgentOfDirectory<T> implements Messaging.Agent.OfTopic<T> {

    private static final Logger log = LoggerFactory.getLogger(AgentOfTopicDirectory.class);

    public AgentOfTopicDirectory(
            MessagingThroughFilesImpl messaging,
            String name,
            DirectoryForTopic<T> topicDirectory,
            Messaging.Receiver<T> receiver) {
        super(messaging, name, topicDirectory, receiver);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Messaging.Topic<T> topic() {
        return (Messaging.Topic<T>) super.destination;
    }

    @Override
    MessageLockAttemptResult tryLockMessage(MessagePath messagePath) {
        Path receivedMarker = this
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
