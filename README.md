# minio-spring-boot-starter
    MINIO Java SDK 封装, 并制作成Spring-boot starter

## 版本说明
 - minio：3.13.0

## 使用方式

### 1.下载源码
 - 下载源码 并install引入使用

### 2.引入依赖
```xml
<dependency>
    <groupId>cn.darkjrong</groupId>
    <artifactId>minio-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 3.配置参数(application.properties)  yml配置

```yaml
minio:
  # 必须为true
  enabled: true
  endpoint: http:192.168.42.131:9000
  access-key: minio
  secret-key: minio123
  bucket-name: data
```
### 4. API 注入
```java
        
    @Autowired
    private MinioTemplate minioTemplate;            

```











