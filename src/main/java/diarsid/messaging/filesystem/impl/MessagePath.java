package diarsid.messaging.filesystem.impl;

import java.nio.file.Path;
import java.util.Objects;

import diarsid.filesystem.api.Directory;

public class MessagePath implements Comparable<MessagePath> {

    public final long offset;
    public final String offsetString;
    public final Path path;

    public MessagePath(Directory directory, long offset) {
        this.offset = offset;
        this.offsetString = String.valueOf(offset);
        this.path = directory.path().resolve(this.offsetString);
    }

    public static MessagePath toMessageOrNull(Path path) {
        String offsetString = path.getFileName().toString();
        long offset = parseNumberOrNegative(offsetString);

        if ( offset < 0 ) {
            return null;
        }
        else {
            return new MessagePath(path, offsetString, offset);
        }
    }

    private MessagePath(Path path, String offsetString, long offset) {
        this.path = path;
        this.offsetString = offsetString;
        this.offset = offset;
    }

    @Override
    public int compareTo(MessagePath other) {
        return Long.compare(this.offset, other.offset);
    }

    public static long parseNumberOrNegative(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            // ignore
            return -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MessagePath)) return false;
        MessagePath that = (MessagePath) o;
        return offset == that.offset &&
                path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, path);
    }


}
