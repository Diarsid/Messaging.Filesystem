package diarsid.messaging.filesystem.impl;

import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diarsid.files.LocalDirectoryWatcher;
import diarsid.files.objects.FileLongSequence;
import diarsid.filesystem.api.Directory;
import diarsid.filesystem.api.File;
import diarsid.messaging.api.Messaging;
import diarsid.messaging.defaultimpl.DestinationAsyncCleaner;
import diarsid.support.concurrency.async.exchange.api.AsyncExchangePoint;
import diarsid.support.concurrency.async.exchange.api.SimpleAsyncConsumer;
import diarsid.support.concurrency.stateful.workers.AbstractStatefulDestroyableWorker;
import diarsid.support.objects.references.Possible;
import diarsid.support.objects.references.References;
import diarsid.support.objects.references.Result;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.util.Objects.isNull;

import static diarsid.messaging.api.Messaging.Destination.Serialization.PersistentType.BYTES;
import static diarsid.messaging.api.Messaging.Destination.Serialization.PersistentType.STRING;
import static diarsid.support.concurrency.async.exchange.api.AsyncExchangePoint.AsyncConsumer.ConcurrencyMode.SEQUENTIAL;

public class DirectoryForDestination<T> extends AbstractStatefulDestroyableWorker implements Messaging.Destination<T> {

    private static final Logger log = LoggerFactory.getLogger(DirectoryForDestination.class);

    public static final String OFFSET_FILE_NAME = ".offset";

    protected final MessagingThroughFilesImpl messaging;
    protected final Messaging.Destination.Type type;
    protected final Serialization<T> serialization;
    protected final FileLongSequence offsetSequence;
    protected final LocalDirectoryWatcher watcher;
    private final AsyncExchangePoint<Path> asyncExchangePoint;
    private final Possible<DestinationAsyncCleaner> cleaner;
    public final Directory directory;

    public DirectoryForDestination(
            MessagingThroughFilesImpl messaging,
            Messaging.Destination.Type type,
            String name,
            Serialization<T> serialization,
            Messaging.Destination.Cleaning cleaning) {
        super(name);
        this.messaging = messaging;
        this.type = type;
        this.serialization = serialization;
        this.directory = this.messaging.resolve(type, name);

        log.info("creating sequence...");
        this.offsetSequence = new FileLongSequence(
                this.directory.path().resolve(OFFSET_FILE_NAME),
                0,
                1,
                () -> lockAndFindMaxOffsetAmongNumericNamedFilesIn(this.directory));

        this.asyncExchangePoint = AsyncExchangePoint.newInstance(this.directory.path().toString());

        this.watcher = new LocalDirectoryWatcher(
                this.directory,
                (eventKind, path) -> {
                    if ( eventKind.equals(ENTRY_CREATE) ) {
                        if ( this.directory.fileSystem().isFile(path) ) {
                            this.asyncExchangePoint.put(path);
                        }
                    }
                });

        if ( isNull(cleaning) ) {
            this.cleaner = References.simplePossibleButEmpty();
        }
        else {
            this.cleaner = References.simplePossibleWith(new DestinationAsyncCleaner(
                    this.messaging,
                    this,
                    cleaning));
        }
    }

    @Override
    protected boolean doSynchronizedStartWork() {
        boolean pointStarted = this.asyncExchangePoint
                .startWork()
                .isInDesiredState;

        boolean watcherStarted = this.watcher
                .startWork()
                .isInDesiredState;

        boolean cleanerStartedOrAbsent =
                this.cleaner.isEmpty() ||
                this.cleaner.get()
                        .startWork()
                        .isInDesiredState;

        return pointStarted && watcherStarted && cleanerStartedOrAbsent;
    }

    @Override
    protected boolean doSynchronizedDestroy() {
        boolean pointDestroyed = this.asyncExchangePoint
                .destroy()
                .isInDesiredState;

        boolean watcherDestroyed = this.watcher
                .destroy()
                .isInDesiredState;

        boolean cleanerDestroyedOrAbsent =
                this.cleaner.isEmpty() ||
                this.cleaner.get()
                        .destroy()
                        .isInDesiredState;

        return pointDestroyed && watcherDestroyed && cleanerDestroyedOrAbsent;
    }

    private static long lockAndFindMaxOffsetAmongNumericNamedFilesIn(Directory directory) {
        log.info("sequence value restoration search...");
        AtomicLong maxOffsetRef = new AtomicLong(0);

        directory.lockAndDo(() -> {
            directory.feedFiles((files) -> {
                long maxOffset = files
                        .stream()
                        .map(File::name)
                        .mapToLong(MessagePath::parseNumberOrNegative)
                        .filter(number -> number > -1)
                        .max()
                        .orElse(0L);
                maxOffsetRef.set(maxOffset);
            });
        });

        long restoredValue = maxOffsetRef.get() + 1;
        log.info("sequence value restored: " + restoredValue);
        return restoredValue;
    }

    @Override
    public final void send(T t) {
        Result<File> result;
        do {
            long offset = this.offsetSequence.getAndIncrement();
            String offsetString = String.valueOf(offset);
            Serialization.PersistentType persistentType = this.serialization.persistentType();
            if ( persistentType.is(BYTES) ) {
                result = this.directory.writeAsFile(offsetString, (Serializable) t);
            }
            else if ( persistentType.is(STRING) ) {
                Object serialized = this.serialization.serialize(t);
                result = this.directory.writeAsFile(offsetString, (Serializable) serialized);
            }
            else {
                throw persistentType.unsupported();
            }
        } while ( result.isEmpty() && result.reason().isInstanceOf(FileAlreadyExistsException.class) );
    }

    @Override
    public Type type() {
        return this.type;
    }

    protected boolean subscribe(String name, Consumer<Path> onNewFileAdded) {
        AsyncExchangePoint.AsyncConsumer<Path> consumer = new SimpleAsyncConsumer<>(
                super.name() + ":" + name,
                onNewFileAdded,
                SEQUENTIAL);
        return this.asyncExchangePoint.consumers().add(consumer);
    }

    protected boolean unsubscribe(String name) {
        return this.asyncExchangePoint.consumers().remove(super.name() + ":" + name);
    }
}
