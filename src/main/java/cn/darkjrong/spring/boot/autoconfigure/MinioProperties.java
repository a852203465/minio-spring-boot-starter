package cn.darkjrong.spring.boot.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * minio属性
 *
 * @author Rong.Jia
 * @date 2021/08/03 22:31:18
 */
@Data
@ConfigurationProperties(prefix = "minio")
public class MinioProperties {

    private static final long DEFAULT_TIMEOUT = 5 * 60 * 1000L;

    /**
     *  是否开启, 默认:false
     */
    private boolean enabled = Boolean.FALSE;

    /**
     * 对象存储服务的URL
     */
    private String endpoint;

    /**
     * Access key就像用户ID，可以唯一标识你的账户。
     */
    private String accessKey;

    /**
     * Secret key是你账户的密码。
     */
    private String secretKey;

    /**
     *  bucket 名称
     */
    private String bucketName;

    /**
     * HTTP连接超时，单位为毫秒。默认：5分钟
     */
    private Long connectTimeout = DEFAULT_TIMEOUT;

    /**
     * HTTP写超时，以毫秒为单位。默认：5分钟
     */
    private Long writeTimeout = DEFAULT_TIMEOUT;

    /**
     *  HTTP读取超时，单位为毫秒。默认：5分钟
     */
    private Long readTimeout = DEFAULT_TIMEOUT;

    /**
     * Metric configuration prefix which are registered on Actuator.
     */
    private String metricName = "minio.storage";


}
