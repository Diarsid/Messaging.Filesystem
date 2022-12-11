package diarsid.messaging.filesystem.defaultimpl.cleaning;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import diarsid.filesystem.api.Directory;
import diarsid.filesystem.api.File;
import diarsid.messaging.defaultimpl.CleaningAtTime;

import static diarsid.messaging.filesystem.impl.DirectoryForDestination.OFFSET_FILE_NAME;

public abstract class CleanFiles extends CleaningAtTime {

    private final Set<String> ignoredFileNames;

    public CleanFiles(int interval, TimeUnit unit) {
        super(interval, unit);
        this.ignoredFileNames = new HashSet<>();
        this.ignoredFileNames.add(OFFSET_FILE_NAME);
    }

    protected final boolean isIgnored(String fileName) {
        return this.ignoredFileNames.contains(fileName);
    }

    protected final void ignore(String fileName) {
        this.ignoredFileNames.add(fileName);
    }

    protected final void remove(Directory directory, long fromOffset, long toOffset) {
        Path directoryPath = directory.path();

        long maxOffsetExcl = toOffset + 1;
        String offsetString;
        for ( long offset = fromOffset; offset < maxOffsetExcl; offset++ ) {
            offsetString = String.valueOf(offset);
            this.delete(directoryPath, offsetString);
        }
    }

    protected final void remove(List<File> files) {
        for ( File file : files ) {
            this.delete(file);
        }
    }

    private void delete(Path directoryPath, String offset) {
        try {
            Files.delete(directoryPath.resolve(offset));
        }
        catch (NoSuchFileException e) {
            LoggerFactory
                    .getLogger(this.getClass())
                    .warn("Message file with offset {} does not exist", offset);
        }
        catch (IOException e) {
            LoggerFactory
                    .getLogger(this.getClass())
                    .error(e.getMessage(), e);
        }
    }

    private void delete(File file) {
        try {
            Files.delete(file.path());
        }
        catch (NoSuchFileException e) {
            LoggerFactory
                    .getLogger(this.getClass())
                    .warn("Message file with offset {} does not exist", file.name());
        }
        catch (IOException e) {
            LoggerFactory
                    .getLogger(this.getClass())
                    .error(e.getMessage(), e);
        }
    }
}
