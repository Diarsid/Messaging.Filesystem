package diarsid.messaging.filesystem.defaultimpl.cleaning;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import diarsid.filesystem.api.Directory;
import diarsid.filesystem.api.File;
import diarsid.messaging.api.Messaging;
import diarsid.messaging.filesystem.impl.DirectoryForDestination;

import static java.time.LocalDateTime.now;

import static diarsid.messaging.filesystem.impl.MessagePath.parseNumberOrNegative;

public class CleanFilesByRemovingOlderThan extends CleanFiles {

    private final Duration duration;

    public CleanFilesByRemovingOlderThan(int interval, TimeUnit unit, Duration duration) {
        super(interval, unit);
        this.duration = duration;
    }

    @Override
    public void clean(Messaging.Destination<?> destination) {
        Directory directory = ((DirectoryForDestination<?>) destination).directory;

        LocalDateTime now = now();
        LocalDateTime threshold = now.minus(this.duration);

        List<File> filesToRemove = new ArrayList<>();

        directory.feedFiles(files -> {
            LocalDateTime created;
            long offset;
            String fileName;
            for ( File file : files ) {
                fileName = file.name();

                if ( super.isIgnored(fileName) ) {
                    continue;
                }

                offset = parseNumberOrNegative(fileName);
                if ( offset < 0 ) {
                    this.ignore(fileName);
                    continue;
                }

                created = file.createdAt();
                if ( created.isBefore(threshold) ) {
                    filesToRemove.add(file);
                }
            }
        });

        super.remove(filesToRemove);
    }
}
