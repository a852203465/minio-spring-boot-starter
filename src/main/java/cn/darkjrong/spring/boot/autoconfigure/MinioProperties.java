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

    /**
     *  是否开启, 默认:false
     */
    private Boolean enabled = Boolean.FALSE;

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
     *  bucket名
     */
    private String bucketName;






}
