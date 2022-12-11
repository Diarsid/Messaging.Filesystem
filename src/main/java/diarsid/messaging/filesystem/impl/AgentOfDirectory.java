package diarsid.messaging.filesystem.impl;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.LoggerFactory;

import diarsid.files.objects.InFile;
import diarsid.filesystem.api.Directory;
import diarsid.messaging.api.Messaging;
import diarsid.messaging.api.exceptions.AgentNotCreatedException;
import diarsid.messaging.api.exceptions.InvalidAgentDataException;
import diarsid.support.concurrency.stateful.workers.AbstractStatefulPausableDestroyableWorker;
import diarsid.support.objects.CommonEnum;
import diarsid.support.objects.references.Possible;
import diarsid.support.objects.references.References;
import diarsid.support.objects.references.Result;

import static java.util.Objects.nonNull;

import static diarsid.messaging.api.Messaging.Destination.Serialization.PersistentType.BYTES;
import static diarsid.messaging.api.Messaging.Destination.Serialization.PersistentType.STRING;
import static diarsid.messaging.api.Messaging.Destination.Type.QUEUE;
import static diarsid.messaging.filesystem.impl.AgentOfDirectory.ReceiveAttemptResult.MESSAGE_CANNOT_BE_RECEIVED_BY_THIS_AGENT;
import static diarsid.messaging.filesystem.impl.AgentOfDirectory.ReceiveAttemptResult.MESSAGE_NOT_RECEIVED;
import static diarsid.messaging.filesystem.impl.AgentOfDirectory.ReceiveAttemptResult.MESSAGE_RECEIVED;
import static diarsid.messaging.filesystem.impl.AgentOfDirectory.ReceiveAttemptResult.MESSAGE_RECEIVED_BY_OTHER_JVM;
import static diarsid.support.concurrency.threads.ThreadsUtil.shutdownAndWait;

public abstract class AgentOfDirectory<T> extends AbstractStatefulPausableDestroyableWorker implements Messaging.Agent {

    private static final String AGENT_DATA_FILE_NAME = ".self.data";
    private static final String AGENT_OFFSET_FILE_NAME = ".self.offset";
    private static final String AGENT_RECEIVED_MARKERS_DIRECTORY_NAME = ".received";

    private static class Data implements Serializable {

        String destinationDirectory;
        String name;
        String canonicalTypeName;
        String serializationPersistentTypeName;
    }

    public static enum ReceiveAttemptResult implements CommonEnum<ReceiveAttemptResult> {
        MESSAGE_RECEIVED,
        MESSAGE_RECEIVED_BY_OTHER_JVM,
        MESSAGE_CANNOT_BE_RECEIVED_BY_THIS_AGENT,
        MESSAGE_NOT_RECEIVED
    }

    public static enum MessageLockAttemptResult implements CommonEnum<MessageLockAttemptResult> {
        MESSAGE_LOCK_ACQUIRED,
        MESSAGE_LOCK_ACQUIRED_BY_ANOTHER_AGENT,
        MESSAGE_LOCK_CANNOT_BE_ACQUIRED;
    }

    private final MessagingThroughFilesImpl messaging;
    private final Messaging.Receiver<T> receiver;
    private final InFile<AgentOfDirectory.Data> agentDataInFile;
    private final InFile<Long> agentOffsetInFile;
    private final Directory agentDirectory;
    private final ExecutorService asyncForTakeFromQueue;
    private final Possible<Future<?>> asyncTakingFromQueue;
    private final Possible<ExecutorService> asyncForPeekOldMessages;
    private final Possible<Future<?>> asyncPeekingOldMessages;
    private final PriorityBlockingQueue<MessagePath> messagePathsQueue;
    private final AtomicBoolean mustPeekOldMessages;
    private final Possible<CountDownLatch> possibleQueueReadiness;
    protected final DirectoryForDestination<T> destination;
    protected final Directory receivedMarkersDirectory;
    private boolean subscribed;

    public AgentOfDirectory(
            MessagingThroughFilesImpl messaging,
            String name,
            DirectoryForDestination<T> destination,
            Messaging.Receiver<T> receiver) {
        super(name);
        this.messaging = messaging;
        this.receiver = receiver;

        this.agentDirectory = messaging
                .agentsDirectory
                .directoryCreateIfNotExists(name)
                .orThrow(() -> new AgentNotCreatedException());

        this.receivedMarkersDirectory = this
                .agentDirectory
                .directoryCreateIfNotExists(AGENT_RECEIVED_MARKERS_DIRECTORY_NAME)
                .orThrow(() -> new AgentNotCreatedException());

        this.destination = destination;
        this.asyncForTakeFromQueue = messaging
                .namedThreadSource
                .newNamedFixedThreadPool(
                        this.destination.type.name() +"[" + this.destination.name() + "]." + Messaging.Agent.class.getSimpleName() + "[" + name + "]",
                        1);
        this.asyncForPeekOldMessages = References.simplePossibleButEmpty();
        this.asyncTakingFromQueue = References.simplePossibleButEmpty();
        this.asyncPeekingOldMessages = References.simplePossibleButEmpty();
        this.messagePathsQueue = new PriorityBlockingQueue<>();
        this.mustPeekOldMessages = new AtomicBoolean(false);
        this.possibleQueueReadiness = References.possiblePropertyButEmpty();
        this.subscribed = false;

        InFile.Initializer<Data> dataInitializer = new InFile.Initializer<>() {

            @Override
            public Class<Data> type() {
                return Data.class;
            }

            @Override
            public Data onFileCreatedGetInitial() {
                AgentOfDirectory.Data initial = new Data();

                initial.destinationDirectory = destination.directory.path().toString();
                initial.name = name;
                initial.canonicalTypeName = destination.serialization.messageType().getCanonicalName();
                initial.serializationPersistentTypeName = destination.serialization.persistentType().name();

                return initial;
            }

            @Override
            public void onFileAlreadyExists(Data existing) {
                AgentOfDirectory.this.validate(existing);
            }
        };

        this.agentDataInFile = new InFile<>(this.agentDirectory, AGENT_DATA_FILE_NAME, dataInitializer);

        InFile.Initializer<Long> offsetInitializer = new InFile.Initializer<>() {

            @Override
            public Class<Long> type() {
                return Long.class;
            }

            @Override
            public Long onFileCreatedGetInitial() {
                return -1L;
            }

            @Override
            public void onFileAlreadyExists(Long existing) {
                AgentOfDirectory.this.mustPeekOldMessages.set(true);
            }
        };

        this.agentOffsetInFile = new InFile<>(this.agentDirectory, AGENT_OFFSET_FILE_NAME, offsetInitializer);
    }

    private void validate(Data data) {
        if ( ! super.name().equals(data.name) ) {
            throw new IllegalStateException();
        }

        if ( ! data.destinationDirectory.equals(this.destination.directory.path().toString()) ) {
            throw new IllegalStateException();
        }

        if ( ! data.canonicalTypeName.equals(this.destination.serialization.messageType().getCanonicalName()) ) {
            throw new IllegalStateException();
        }

        if ( ! data.serializationPersistentTypeName.equals(this.destination.serialization.persistentType().name()) ) {
            throw new IllegalStateException();
        }
    }

    @Override
    protected boolean doSynchronizedStartWork() {
        if ( this.mustPeekOldMessages.get() ) {
            this.possibleQueueReadiness.resetTo(new CountDownLatch(1));
            this.asyncForPeekOldMessages.ifPresent((async) -> {
                this.asyncPeekingOldMessages.ifPresent(job -> job.cancel(true));
            });
            this.asyncForPeekOldMessages.nullify();
            this.asyncForPeekOldMessages.ifNotPresentResetTo(this
                    .messaging
                    .namedThreadSource
                    .newNamedFixedThreadPool(
                            this.destination.type.name() +"[" + this.destination.name() + "]." + Messaging.Agent.class.getSimpleName() + "[" + super.name() + "].peekOldMessages",
                            1));

            this.asyncPeekingOldMessages.resetTo(this.asyncForPeekOldMessages.orThrow().submit(() -> {
                long offset = this.agentOffsetInFile.read();
                long oldMessagesMaxOffset = this.destination.offsetSequence.get() - 1;
                MessagePath messagePath;
                boolean allOldMessagesRead = false;
                while ( true ) {
                    if ( ! super.isWorkingOrTransitingToWorking() ) {
                        break;
                    }

                    offset++;

                    if ( offset <= oldMessagesMaxOffset ) {
                        messagePath = new MessagePath(this.destination.directory, offset);
                        this.messagePathsQueue.put(messagePath);
                    }
                    else {
                        allOldMessagesRead = true;
                        break;
                    }
                }

                if ( allOldMessagesRead ) {
                    this.possibleQueueReadiness.get().countDown();
                }
            }));
        }

        this.asyncTakingFromQueue.resetTo(this.asyncForTakeFromQueue.submit(this::awaitAndTakeFileMessageFromQueue));

        this.subscribed = this.destination.subscribe(
                super.name(),
                (path) -> {
                    MessagePath messagePath = MessagePath.toMessageOrNull(path);
                    if ( nonNull(messagePath) ) {
                        this.messagePathsQueue.put(messagePath);
                    }
                });

        return this.subscribed;
    }

    @Override
    protected boolean doSynchronizedPauseWork() {
        this.subscribed = this.destination.unsubscribe(super.name());

        this.asyncTakingFromQueue.ifPresent(job -> job.cancel(true));
        this.asyncTakingFromQueue.nullify();

        if ( this.mustPeekOldMessages.get() ) {
            this.asyncPeekingOldMessages.ifPresent(job -> job.cancel(true));
        }

        this.mustPeekOldMessages.set(true);

        return ! this.subscribed;
    }

    @Override
    protected boolean doSynchronizedDestroy() {
        if ( this.subscribed ) {
            return this.destination.unsubscribe(super.name());
        }
        this.asyncTakingFromQueue.ifPresent(job -> job.cancel(true));
        this.asyncTakingFromQueue.nullify();
        shutdownAndWait(this.asyncForTakeFromQueue);

        return true;
    }

    private void awaitAndTakeFileMessageFromQueue() {
        if ( this.possibleQueueReadiness.isPresent() ) {
            try {
                this.possibleQueueReadiness.get().await();
            }
            catch (InterruptedException e) {
                return;
            }
        }

        while ( this.isWorkingOrTransitingToWorking() ) {
            try {
                MessagePath messagePath = this.messagePathsQueue.take();
                this.processSafely(messagePath);
            }
            catch (Throwable e) {
                LoggerFactory
                        .getLogger(this.getClass())
                        .error("Cannot take " + MessagePath.class.getSimpleName() + " from priority queue!", e);
            }
        }
    }

    private void processSafely(MessagePath messagePath) {
        try {
            ReceiveAttemptResult decision = this.tryToLockAndReceive(messagePath);

            boolean saveOffset =
                    decision.is(MESSAGE_RECEIVED) ||
                    this.destination.type.is(QUEUE) && decision.is(MESSAGE_CANNOT_BE_RECEIVED_BY_THIS_AGENT);

            if ( saveOffset ) {
                this.save(messagePath.offset);
            }
        }
        catch (Throwable t) {
            LoggerFactory
                    .getLogger(this.getClass())
                    .error("Cannot process message: ", t);
        }
    }

    private void save(long offset) {
        this.agentOffsetInFile.write(offset);
    }

    private ReceiveAttemptResult tryToLockAndReceive(MessagePath messagePath)  {
        MessageLockAttemptResult messageLockAttemptResult = this.tryLockMessage(messagePath);

        if ( messageLockAttemptResult.is(MessageLockAttemptResult.MESSAGE_LOCK_ACQUIRED_BY_ANOTHER_AGENT) ) {
            return MESSAGE_RECEIVED_BY_OTHER_JVM;
        }
        else if ( messageLockAttemptResult.is(MessageLockAttemptResult.MESSAGE_LOCK_CANNOT_BE_ACQUIRED) ) {
            return MESSAGE_NOT_RECEIVED;
        }

        Messaging.Destination.Serialization<T> serialization = this.destination.serialization;

        if ( serialization.persistentType().is(STRING) ) {
            Result<Object> readResult = this.destination.directory.readFromFile(messagePath.offsetString);
            if ( readResult.isPresent() ) {
                try {
                    Object read = readResult.get();
                    try {
                        T t = serialization.deserialize(read);
                        this.receiver.receive(messagePath.offset, t);
                        return MESSAGE_RECEIVED;
                    }
                    catch (Throwable t) {
                        LoggerFactory
                                .getLogger(this.getClass())
                                .error("Exception during message receiving: ", t);
                        return ReceiveAttemptResult.MESSAGE_NOT_RECEIVED;
                    }
                }
                catch (Throwable t) {
                    LoggerFactory
                            .getLogger(this.getClass())
                            .error("Exception during message receiving: ", t);
                    return ReceiveAttemptResult.MESSAGE_NOT_RECEIVED;
                }
            }
            else {
                Result.Reason reason = readResult.reason();
                if ( reason.isExceptional() ) {
                    LoggerFactory
                            .getLogger(this.getClass())
                            .error("Exception during message receiving: ", reason.subjectAs(Throwable.class));
                }
                else if ( reason.is(String.class) ) {
                    LoggerFactory
                            .getLogger(this.getClass())
                            .error("Cannot read message, reason: " + reason.subjectAs(String.class));
                }
                else {
                    LoggerFactory
                            .getLogger(this.getClass())
                            .error("Cannot read message, reason: " + reason.subject().toString());
                }
                return MESSAGE_CANNOT_BE_RECEIVED_BY_THIS_AGENT;
            }
        }
        else if ( serialization.persistentType().is(BYTES) ) {
            Result<T> readResult = this.destination.directory.readFromFile(messagePath.offsetString, serialization.messageType());
            if ( readResult.isPresent() ) {
                try {
                    this.receiver.receive(messagePath.offset, readResult.get());
                    return MESSAGE_RECEIVED;
                }
                catch (Throwable t) {
                    LoggerFactory
                            .getLogger(this.getClass())
                            .error("Exception during message receiving: ", t);
                    return ReceiveAttemptResult.MESSAGE_NOT_RECEIVED;
                }
            }
            else {
                Result.Reason reason = readResult.reason();
                if ( reason.isExceptional() ) {
                    LoggerFactory
                            .getLogger(this.getClass())
                            .error("Exception during message receiving: ", reason.subjectAs(Throwable.class));
                }
                else if ( reason.is(String.class) ) {
                    LoggerFactory
                            .getLogger(this.getClass())
                            .error("Cannot read message, reason: " + reason.subjectAs(String.class));
                }
                else {
                    LoggerFactory
                            .getLogger(this.getClass())
                            .error("Cannot read message, reason: " + reason.subject().toString());
                }
                return MESSAGE_CANNOT_BE_RECEIVED_BY_THIS_AGENT;
            }
        }
        else {
            LoggerFactory
                    .getLogger(this.getClass())
                    .error("Exception during message receiving: {} {} is not supported!",
                            serialization.persistentType(),
                            serialization.persistentType().getClass().getSimpleName());
            return ReceiveAttemptResult.MESSAGE_NOT_RECEIVED;
        }
    }

    abstract MessageLockAttemptResult tryLockMessage(MessagePath messagePath);

    @Override
    public LocalDateTime createdAt() {
        try {
            Instant instant = Files.getLastModifiedTime(this.agentDataInFile.path()).toInstant();
            return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        }
        catch (IOException e) {
            throw new InvalidAgentDataException(e);
        }
    }

}
