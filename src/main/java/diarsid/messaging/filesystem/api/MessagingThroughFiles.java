package diarsid.messaging.filesystem.api;

import java.nio.file.Path;

import diarsid.files.PathBearer;
import diarsid.messaging.api.Messaging;
import diarsid.messaging.filesystem.impl.MessagingThroughFilesImpl;

public interface MessagingThroughFiles extends Messaging, PathBearer {

    static Messaging filesystemMessaging(String name) {
        return new MessagingThroughFilesImpl(name);
    }

    static Messaging filesystemMessaging(String name, Path root) {
        return new MessagingThroughFilesImpl(name, root);
    }
}
