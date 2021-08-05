package cn.darkjrong.spring.boot.autoconfigure;

import cn.darkjrong.minio.MinioTemplate;
import cn.hutool.core.lang.Assert;
import io.minio.MinioClient;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.net.URL;


/**
 * minio工厂bean
 *
 * @author Rong.Jia
 * @date 2021/08/05 10:41:45
 */
public class MinioFactoryBean implements FactoryBean<MinioTemplate>, InitializingBean {

    private MinioTemplate minioTemplate;
    private final MinioProperties minioProperties;

    public MinioFactoryBean(MinioProperties minioProperties) {
        this.minioProperties = minioProperties;
    }

    @Override
    public MinioTemplate getObject() {
        return minioTemplate;
    }

    @Override
    public Class<?> getObjectType() {
        return MinioTemplate.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        String endpoint = minioProperties.getEndpoint();
        String secretKey = minioProperties.getSecretKey();
        String accessKey = minioProperties.getAccessKey();
        String bucketName = minioProperties.getBucketName();

        Assert.notBlank(endpoint, "'endpoint' cannot be empty");
        Assert.notBlank(secretKey, "'secretKey' cannot be empty");
        Assert.notBlank(accessKey, "'accessKey' cannot be empty");
        Assert.notBlank(bucketName, "'bucketName' cannot be empty");

        MinioClient minioClient = MinioClient.builder().endpoint(new URL(endpoint))
                .credentials(accessKey, secretKey).build();

        minioTemplate = new MinioTemplate(minioClient, minioProperties);

        if (!minioTemplate.bucketExists(bucketName)) {
            minioTemplate.makeBucket(bucketName);
        }

    }
}
