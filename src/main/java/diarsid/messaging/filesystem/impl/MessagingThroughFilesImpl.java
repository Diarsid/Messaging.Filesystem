package diarsid.messaging.filesystem.impl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import diarsid.filesystem.api.Directory;
import diarsid.filesystem.api.FileSystem;
import diarsid.messaging.filesystem.api.MessagingThroughFiles;
import diarsid.messaging.defaultimpl.MessagingThreads;
import diarsid.support.concurrency.stateful.workers.AbstractStatefulDestroyableWorker;
import diarsid.support.concurrency.threads.NamedThreadSource;
import diarsid.support.objects.workers.Worker;

import static diarsid.filesystem.api.FileSystem.DEFAULT_INSTANCE;

public class MessagingThroughFilesImpl
        extends AbstractStatefulDestroyableWorker
        implements MessagingThroughFiles, MessagingThreads {

    private final Path messagingRootDirectory;
    private final List<Worker.Destroyable> destinationWorkers;
    private final List<Worker.Destroyable> agentWorkers;
    public final FileSystem fileSystem;
    public final Directory agentsDirectory;
    public final NamedThreadSource namedThreadSource;

    public MessagingThroughFilesImpl(String name) {
        this(name, Paths
                .get(System.getProperty("user.home"))
                .resolve(".java")
                .resolve("messaging")
                .resolve(name));

        this.startWork();
    }

    public MessagingThroughFilesImpl(String name, Path messagingRootDirectory) {
        super(name);
        this.messagingRootDirectory = messagingRootDirectory;
        this.namedThreadSource = new NamedThreadSource("Messaging[" + this.messagingRootDirectory.toString() + "]");
        this.destinationWorkers = new ArrayList<>();
        this.agentWorkers = new ArrayList<>();
        this.fileSystem = DEFAULT_INSTANCE;

        Path agentDir = this.messagingRootDirectory
                .resolve(Agent.class.getSimpleName());

        this.agentsDirectory = this.fileSystem
                .toDirectoryCreateIfNotExists(agentDir)
                .orThrow();
    }

    @Override
    protected boolean doSynchronizedStartWork() {
        return true;
    }

    @Override
    protected boolean doSynchronizedDestroy() {
        for ( Worker.Destroyable agent : this.agentWorkers) {
            agent.destroy();
        }

        for ( Worker.Destroyable destination : this.destinationWorkers) {
            destination.destroy();
        }

        return true;
    }

    @Override
    public NamedThreadSource threads() {
        return this.namedThreadSource;
    }

    void add(AgentOfDirectory<?> agent) {
        super.doSynchronizedVoidChange(() -> {
            if ( super.state().current().doesNotAllowWork() ) {
                throw new IllegalStateException();
            }
            this.destinationWorkers.add(agent);
        });
    }

    @Override
    public <T> Queue<T> queue(String name, Class<T> type) {
        Destination.Serialization<T> serialization = new SerializationAsObject<>(type);
        return this.queue(name, serialization, null);
    }

    @Override
    public <T> Queue<T> queue(String name, Destination.Serialization<T> serialization) {
        return this.queue(name, serialization, null);
    }

    @Override
    public <T> Queue<T> queue(String name, Class<T> type, Destination.Cleaning cleaning) {
        Destination.Serialization<T> serialization = new SerializationAsObject<>(type);
        return this.queue(name, serialization, cleaning);
    }

    @Override
    public <T> Queue<T> queue(String name, Destination.Serialization<T> serialization, Destination.Cleaning cleaning) {
        return super.doSynchronizedReturnChange(() -> {
            if ( super.state().current().doesNotAllowWork() ) {
                throw new IllegalStateException();
            }
            DirectoryForQueue<T> queueDirectory = new DirectoryForQueue<>(this, name, serialization, cleaning);
            this.destinationWorkers.add(queueDirectory);
            queueDirectory.startWork();
            return queueDirectory;
        });
    }

    @Override
    public <T> Topic<T> topic(String name, Class<T> type) {
        Destination.Serialization<T> serialization = new SerializationAsObject<>(type);
        return this.topic(name, serialization, null);
    }

    @Override
    public <T> Topic<T> topic(String name, Destination.Serialization<T> serialization) {
        return this.topic(name, serialization, null);
    }

    @Override
    public <T> Topic<T> topic(String name, Class<T> type, Destination.Cleaning cleaning) {
        Destination.Serialization<T> serialization = new SerializationAsObject<>(type);
        return this.topic(name, serialization, cleaning);
    }

    @Override
    public <T> Topic<T> topic(String name, Destination.Serialization<T> serialization, Destination.Cleaning cleaning) {
        return super.doSynchronizedReturnChange(() -> {
            if ( super.state().current().doesNotAllowWork() ) {
                throw new IllegalStateException();
            }

            DirectoryForTopic<T> topicDirectory = new DirectoryForTopic<>(this, name, serialization, cleaning);
            this.destinationWorkers.add(topicDirectory);
            topicDirectory.startWork();
            return topicDirectory;
        });
    }

    Directory resolve(Destination.Type type, String name) {
        Path destinationDir = this.messagingRootDirectory
                .resolve(Destination.class.getSimpleName())
                .resolve(type.name())
                .resolve(name);

        return this.fileSystem
                .toDirectoryCreateIfNotExists(destinationDir)
                .orThrow();
    }

    @Override
    public Path path() {
        return this.messagingRootDirectory;
    }
}
