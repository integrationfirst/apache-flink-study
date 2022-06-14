package vn.ifa.study.flink.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import vn.ifa.study.flink.model.Document;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DocumentCompletionEvent {
    private Document document;
    private String completionTime;
    private String operatorId;
}
