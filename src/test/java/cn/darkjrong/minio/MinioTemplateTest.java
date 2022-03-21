package cn.darkjrong.minio;

import cn.darkjrong.spring.boot.autoconfigure.MinioAutoConfiguration;
import cn.darkjrong.spring.boot.autoconfigure.MinioFactoryBean;
import cn.darkjrong.spring.boot.autoconfigure.MinioProperties;
import io.minio.messages.Bucket;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

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
        properties.setEndpoint("http://10.20.54.133:9000");
        properties.setAccessKey("minio");
        properties.setSecretKey("minio123");
        properties.setBucketName("test");

        MinioFactoryBean minioFactoryBean = new MinioAutoConfiguration(properties).minioFactoryBean();
        minioFactoryBean.afterPropertiesSet();
        minioTemplate = minioFactoryBean.getObject();

    }


    @Test
    public void bucketExists() {

        System.out.println(minioTemplate.bucketExists("test"));

    }

    @Test
    public void uploadFile() {

        System.out.println(minioTemplate.putObject(new File("F:\\我的图片\\桌面\\1.jpeg")));

    }

    @Test
    public void downloadFile() {

        minioTemplate.downloadObject("1.jpeg", "F:\\我的图片\\桌面\\1.jpeg");

    }

    @Test
    public void getFileUrl() {

        System.out.println(minioTemplate.getObjectUrl("20220321/d3ce87f32d1d4521aa75def974a044b21647845867860.jpeg"));

    }

    @Test
    public void copyObject() {

        String object = minioTemplate.copyObject("scm", "test", "/2022/03/03/08c0c0ae75a04532b5ac19611c776a8f.docx");
        System.out.println(object);

    }


    @Test
    public void listBuckets() {
        List<Bucket> bucketList = minioTemplate.listBuckets();
        for (Bucket bucket : bucketList) {
            System.out.println(bucket.name());
        }
    }

    @Test
    public void copyObject1() {

        System.out.println(minioTemplate.copyObject("20220321/420ec82ff3624a059d326a9a812d83481647844979194.jpeg"));


    }




}
