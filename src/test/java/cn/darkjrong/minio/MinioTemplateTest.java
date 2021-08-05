package cn.darkjrong.minio;

import cn.darkjrong.spring.boot.autoconfigure.MinioAutoConfiguration;
import cn.darkjrong.spring.boot.autoconfigure.MinioFactoryBean;
import cn.darkjrong.spring.boot.autoconfigure.MinioProperties;
import io.minio.BucketExistsArgs;
import io.minio.MinioClient;
import io.minio.SetBucketPolicyArgs;
import io.minio.errors.*;
import org.junit.Before;
import org.junit.Test;
import sun.net.www.content.image.jpeg;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * minio 测试
 *
 * @author Rong.Jia
 * @date 2021/08/03 22:34:34
 */
public class MinioTemplateTest {

    private MinioTemplate minioTemplate;

    @Before
    public void before() throws Exception {
        MinioProperties properties = new MinioProperties();
        properties.setEnabled(true);
        properties.setEndpoint("http://192.168.42.133:9000");
        properties.setAccessKey("minio");
        properties.setSecretKey("minio123");
        properties.setBucketName("data");

        MinioFactoryBean minioFactoryBean = new MinioAutoConfiguration(properties).minioFactoryBean();
        minioFactoryBean.afterPropertiesSet();
        minioTemplate = minioFactoryBean.getObject();

    }


    @Test
    public void bucketExists() {

        System.out.println(minioTemplate.bucketExists("data"));

    }

    @Test
    public void uploadFile() {

        minioTemplate.putObject(new File("F:\\我的图片\\1.jpeg"));

    }

    @Test
    public void downloadFile() {

        minioTemplate.downloadObject("1.jpeg", "F:\\我的图片\\3.jpeg");

    }

    @Test
    public void getFileUrl() {

        System.out.println(minioTemplate.getObjectUrl("1.jpeg"));

    }











}
