package cn.darkjrong.minio.domain;

import lombok.Getter;

/**
 * bucket 版本状态
 *
 * @author Rong.Jia
 * @date 2021/08/05 08:51:54
 */
@Getter
public enum BucketVersionStatus {

    // 开启
    ENABLED("Enabled"),
    SUSPENDED("Suspended");

    private String value;

    BucketVersionStatus(String value) {
        this.value = value;
    }


}
