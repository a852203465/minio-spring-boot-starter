package cn.darkjrong.minio.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * bucket 版本状态
 *
 * @author Rong.Jia
 * @date 2021/08/05 08:51:54
 */
@Getter
@AllArgsConstructor
public enum BucketVersionStatus {

    // 开启
    ENABLED("Enabled"),
    SUSPENDED("Suspended");

    private final String value;


}
