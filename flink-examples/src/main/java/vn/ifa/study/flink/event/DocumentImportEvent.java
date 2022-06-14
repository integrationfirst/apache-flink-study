package vn.ifa.study.flink.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import vn.ifa.study.flink.model.Document;

@Setter
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocumentImportEvent {

    private Document document;
    private String importTime;

}
