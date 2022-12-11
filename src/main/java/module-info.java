module diarsid.messaging.filesystem {

    requires org.slf4j;
    requires diarsid.support;
    requires diarsid.messaging.api;
    requires diarsid.filesystem;

    exports diarsid.messaging.filesystem.api;
    exports diarsid.messaging.filesystem.defaultimpl.cleaning;
    exports diarsid.messaging.filesystem.defaultimpl.serialization;
}
