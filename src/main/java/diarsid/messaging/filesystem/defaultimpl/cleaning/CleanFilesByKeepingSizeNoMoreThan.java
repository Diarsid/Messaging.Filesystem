package diarsid.messaging.filesystem.defaultimpl.cleaning;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diarsid.filesystem.api.Directory;
import diarsid.filesystem.api.File;
import diarsid.messaging.api.Messaging;
import diarsid.messaging.filesystem.impl.DirectoryForDestination;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;

import static diarsid.messaging.filesystem.impl.MessagePath.parseNumberOrNegative;

public class CleanFilesByKeepingSizeNoMoreThan extends CleanFiles {

    private static final Logger log = LoggerFactory.getLogger(CleanFilesByKeepingSizeNoMoreThan.class);

    private final int thresholdSize;

    public CleanFilesByKeepingSizeNoMoreThan(int interval, TimeUnit unit, int thresholdSize) {
        super(interval, unit);
        this.thresholdSize = thresholdSize;
    }

    @Override
    public void clean(Messaging.Destination<?> destination) {
        Directory directory = ((DirectoryForDestination<?>) destination).directory;

        AtomicLong from = new AtomicLong();
        AtomicLong to = new AtomicLong();

        directory
                .feedFiles(files -> {
                    long minOffset = MAX_VALUE;
                    long maxOffset = MIN_VALUE;
                    long totalQty = 0;
                    String fileName;

                    for ( File file : files ) {
                        fileName = file.name();

                        if ( super.isIgnored(fileName) ) {
                            continue;
                        }

                        long offset = parseNumberOrNegative(fileName);
                        if ( offset < 0 ) {
                            super.ignore(fileName);
                            continue;
                        }

                        if ( offset > maxOffset ) {
                            maxOffset = offset;
                        }

                        if ( offset < minOffset ) {
                            minOffset = offset;
                        }

                        totalQty++;
                    }

                    if ( totalQty < this.thresholdSize) {
                        return;
                    }

                    long nToRemove = totalQty - this.thresholdSize;
                    long maxOffsetToRemove = minOffset + nToRemove;

                    from.set(minOffset);
                    to.set(maxOffsetToRemove);
                });

        super.remove(directory, from.get(), to.get());
    }
}
