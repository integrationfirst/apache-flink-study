package vn.ifa.study.flink.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import vn.ifa.study.flink.model.UserInfo;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserAddedEvent {
    private UserInfo userInfo;
    private String addedTime;
}
