package vn.ifa.study.flink;

import java.time.LocalDateTime;

import org.joda.time.DateTime;

public final class ExampleData {

    private ExampleData() {

    }

    public static final DocumentImportEvent[] IMPORT_EVENTS = {

            DocumentImportEvent.builder()
                               .importTime(LocalDateTime.now()
                                                        .toString())
                               .document(Document.builder()
                                                 .id("1")
                                                 .name("DOC1")
                                                 .build())
                               .build(),
            DocumentImportEvent.builder()
                               .importTime(DateTime.now()
                                                   .toString())
                               .document(Document.builder()
                                                 .id("2")
                                                 .name("DOC2")
                                                 .build())
                               .build(),
            DocumentImportEvent.builder()
                               .importTime(DateTime.now()
                                                   .toString())
                               .document(Document.builder()
                                                 .id("3")
                                                 .name("DOC3")
                                                 .build())
                               .build()

    };
}
