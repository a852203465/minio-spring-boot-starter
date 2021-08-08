package cn.darkjrong.spring.boot.autoconfigure;

import cn.darkjrong.minio.MinioHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * minio自动配置
 *
 * @author Rong.Jia
 * @date 2021/08/03 22:32:11
 */
@Configuration
@Import(MinioHealthIndicator.class)
@ConditionalOnClass({MinioProperties.class})
@EnableConfigurationProperties({MinioProperties.class})
@ConditionalOnProperty(prefix = "minio", name = "enabled", havingValue = "true")
public class MinioAutoConfiguration {

    private final MinioProperties minioProperties;

    public MinioAutoConfiguration(MinioProperties minioProperties) {
        this.minioProperties = minioProperties;
    }

    @Bean
    public MinioFactoryBean minioFactoryBean() {
        return new MinioFactoryBean(minioProperties);
    }




}