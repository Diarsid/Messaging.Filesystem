package diarsid.messaging.filesystem.defaultimpl.serialization;

import java.util.function.Function;

import diarsid.messaging.api.Messaging;

import static diarsid.messaging.api.Messaging.Destination.Serialization.PersistentType.STRING;

public class SerializationAsString<T> implements Messaging.Destination.Serialization<T> {

    private final Class<T> type;
    private final Function<String, T> stringToT;
    private final Function<T, String> tToString;

    public SerializationAsString(
            Class<T> type,
            Function<String, T> stringToT,
            Function<T, String> tToString) {
        this.type = type;
        this.stringToT = stringToT;
        this.tToString = tToString;
    }

    @Override
    public Class<T> messageType() {
        return this.type;
    }

    @Override
    public PersistentType persistentType() {
        return STRING;
    }

    @Override
    public T deserialize(Object stored) {
        return this.stringToT.apply((String) stored);
    }

    @Override
    public Object serialize(T message) {
        return this.tToString.apply(message);
    }
}
