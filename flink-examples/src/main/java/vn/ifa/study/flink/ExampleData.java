package vn.ifa.study.flink;

import java.time.LocalDateTime;

import org.joda.time.DateTime;

import vn.ifa.study.flink.event.DocumentCompletionEvent;
import vn.ifa.study.flink.event.DocumentImportEvent;
import vn.ifa.study.flink.model.Document;

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

    public static final DocumentCompletionEvent[] COMPLETION_EVENTS = {

            DocumentCompletionEvent.builder()
                                   .completionTime(LocalDateTime.now()
                                                                .toString())
                                   .document(Document.builder()
                                                     .id("1")
                                                     .name("DOC1")
                                                     .build())
                                   .build(),
            DocumentCompletionEvent.builder()
                                   .completionTime(LocalDateTime.now()
                                                                .toString())
                                   .document(Document.builder()
                                                     .id("2")
                                                     .name("DOC2")
                                                     .build())
                                   .build(),
            DocumentCompletionEvent.builder()
                                   .completionTime(LocalDateTime.now()
                                                                .toString())
                                   .document(Document.builder()
                                                     .id("4")
                                                     .name("DOC4")
                                                     .build())
                                   .build() };

}
