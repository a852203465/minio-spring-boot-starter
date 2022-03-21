package cn.darkjrong.minio.config;

import cn.darkjrong.minio.MinioTemplate;
import cn.darkjrong.spring.boot.autoconfigure.MinioProperties;
import cn.hutool.core.util.ObjectUtil;
import org.springframework.boot.actuate.autoconfigure.web.server.ManagementContextAutoConfiguration;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Component;

/**
 * 设置Minio运行状况指示器
 *
 * @author Rong.Jia
 * @date 2021/08/08 19:24:24
 */
@Component
@ConditionalOnClass(ManagementContextAutoConfiguration.class)
public class MinioHealthIndicator implements HealthIndicator {

    private final MinioTemplate minioTemplate;
    private final MinioProperties minioProperties;

    public MinioHealthIndicator(MinioTemplate minioTemplate, MinioProperties minioProperties) {
        this.minioTemplate = minioTemplate;
        this.minioProperties = minioProperties;
    }

    @Override
    public Health health() {
        if (ObjectUtil.isNull(minioTemplate)) {
            return Health.down().build();
        }

        try {

           if (minioTemplate.bucketExists(minioProperties.getBucketName())) {
               return Health.up()
                       .withDetail("bucketName", minioProperties.getBucketName())
                       .build();
           }else {
               return Health.down()
                       .withDetail("bucketName", minioProperties.getBucketName())
                       .build();
           }
        } catch (Exception e) {
            return Health.down(e)
                    .withDetail("bucketName", minioProperties.getBucketName())
                    .build();
        }
    }
}
