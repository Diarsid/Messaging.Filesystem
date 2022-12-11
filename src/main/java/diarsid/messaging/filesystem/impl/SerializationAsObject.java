package diarsid.messaging.filesystem.impl;

import diarsid.messaging.api.Messaging;

import static diarsid.messaging.api.Messaging.Destination.Serialization.PersistentType.BYTES;

public class SerializationAsObject<T> implements Messaging.Destination.Serialization<T> {

    private final Class<T> type;

    public SerializationAsObject(Class<T> type) {
        this.type = type;
    }

    @Override
    public Class<T> messageType() {
        return this.type;
    }

    @Override
    public PersistentType persistentType() {
        return BYTES;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(Object stored) {
        return (T) stored;
    }

    @Override
    public Object serialize(T message) {
        return message;
    }
}
