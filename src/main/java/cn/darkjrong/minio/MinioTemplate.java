package cn.darkjrong.minio;

import cn.darkjrong.minio.domain.BucketPolicyParam;
import cn.darkjrong.minio.domain.BucketVersionStatus;
import cn.darkjrong.minio.domain.ListObjectParam;
import cn.darkjrong.minio.domain.RemoveObject;
import cn.darkjrong.minio.exceptions.MinioException;
import cn.darkjrong.spring.boot.autoconfigure.MinioProperties;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.system.SystemUtil;
import com.alibaba.fastjson.JSON;
import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * minio 操作
 *
 * @author Rong.Jia
 * @date 2021/08/03 22:26:43
 */
public class MinioTemplate {

    private static final Logger logger = LoggerFactory.getLogger(MinioTemplate.class);
    private static final String DATA_TMP = SystemUtil.get(SystemUtil.TMPDIR);

    private final MinioClient minioClient;
    private final MinioProperties minioProperties;

    public MinioTemplate(MinioClient minioClient, MinioProperties minioProperties) {
        this.minioClient = minioClient;
        this.minioProperties = minioProperties;
    }

    public MinioClient getMinioClient() {
        return minioClient;
    }

    /**
     * 判断桶是否存在
     *
     * @param bucketName bucket名称
     * @return {@link Boolean} 是否存在
     */
    public Boolean bucketExists(String bucketName) {

        BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            return minioClient.bucketExists(bucketExistsArgs);
        } catch (Exception e) {
            logger.error("bucketExists {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 获取对象信息
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link StatObjectResponse}
     * @throws MinioException minio异常
     */
    public StatObjectResponse statObject(String bucketName, String objectName) throws MinioException {
        return this.statObject(bucketName, objectName, null);
    }

    /**
     * 获取对象信息
     *
     * @param objectName 对象名称
     * @return {@link StatObjectResponse}
     * @throws MinioException minio异常
     */
    public StatObjectResponse statObject(String objectName) throws MinioException {
        return this.statObject(minioProperties.getBucketName(), objectName);
    }

    /**
     * 获取对象信息
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @param versionId  版本标识
     * @return {@link StatObjectResponse}
     * @throws MinioException minio异常
     */
    public StatObjectResponse statObject(String bucketName, String objectName, String versionId) throws MinioException {

        StatObjectArgs statObjectArgs;
        if (StrUtil.isBlank(versionId)) {
            statObjectArgs = StatObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .build();
        } else {
            statObjectArgs = StatObjectArgs.builder()
                    .bucket(bucketName)
                    .versionId(versionId)
                    .object(objectName)
                    .build();
        }

        try {
            return minioClient.statObject(statObjectArgs);
        } catch (Exception e) {
            logger.error("statObject {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }

    }

    /**
     * 获取对象
     *
     * @param objectName 对象名称
     * @return {@link byte[]} 对象字节数组
     * @throws MinioException minio异常
     */
    public byte[] getObject(String objectName) throws MinioException {
        return this.getObject(minioProperties.getBucketName(), objectName);
    }

    /**
     * 获取对象
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link byte[]} 对象字节数组
     * @throws MinioException minio异常
     */
    public byte[] getObject(String bucketName, String objectName) throws MinioException {

        GetObjectArgs getObjectArgs = GetObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();

        try {
            return IoUtil.readBytes(minioClient.getObject(getObjectArgs));
        } catch (Exception e) {
            logger.error("getObject {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 下载对象
     *
     * @param bucketName bucket名称
     * @param fileName   文件全限定路径名
     * @param objectName 对象名称
     * @throws MinioException minio异常
     */
    public void downloadObject(String bucketName, String objectName, String fileName) throws MinioException {

        DownloadObjectArgs args = DownloadObjectArgs.builder()
                .filename(fileName)
                .object(objectName)
                .bucket(bucketName)
                .build();

        try {
            minioClient.downloadObject(args);
        } catch (Exception e) {
            logger.error("downloadObject {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 下载对象
     *
     * @param fileName   文件全限定路径名
     * @param objectName 对象名称
     * @throws MinioException minio异常
     */
    public void downloadObject(String objectName, String fileName) throws MinioException {
        this.downloadObject(minioProperties.getBucketName(), objectName, fileName);
    }

    /**
     * 复制对象
     *
     * <p>
     * 将objectName从srcBucketName复制到targetBucketName
     * </p>
     *
     * @param srcBucketName    源bucket名称
     * @param targetBucketName 目标bucket 名称
     * @param objectName       对象名称
     * @throws MinioException minio异常
     */
    public void copyObject(String srcBucketName, String targetBucketName, String objectName) throws MinioException {

        CopySource copySource = CopySource.builder()
                .bucket(srcBucketName)
                .build();

        CopyObjectArgs copyObjectArgs = CopyObjectArgs.builder()
                .bucket(targetBucketName)
                .object(objectName)
                .source(copySource)
                .build();

        try {
            minioClient.copyObject(copyObjectArgs);
        } catch (Exception e) {
            logger.error("copyObject {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }

    }

    /**
     * 复制对象
     *
     * <p>
     * 将objectName从srcBucketName复制到targetBucketName的targetObjectName
     * </p>
     *
     * @param srcBucketName    源bucket名称
     * @param targetBucketName 目标bucket 名称
     * @param srcObjectName    源对象名称
     * @param srcObjectName    目标对象名称
     * @throws MinioException minio异常
     */
    public void copyObject(String srcBucketName, String targetBucketName, String srcObjectName, String targetObjectName) throws MinioException {

        CopySource copySource = CopySource.builder()
                .bucket(srcBucketName)
                .object(srcObjectName)
                .build();

        CopyObjectArgs copyObjectArgs = CopyObjectArgs.builder()
                .bucket(targetBucketName)
                .object(targetObjectName)
                .source(copySource)
                .build();

        try {
            minioClient.copyObject(copyObjectArgs);
        } catch (Exception e) {
            logger.error("copyObject {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }

    }

    /**
     * 获得对象url
     *
     * @param duration   超时时长
     * @param unit       单位
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link String} 对象url
     */
    public String getObjectUrl(String bucketName, String objectName, int duration, TimeUnit unit) {

        GetPresignedObjectUrlArgs objectUrlArgs = GetPresignedObjectUrlArgs.builder()
                .method(Method.GET)
                .bucket(bucketName)
                .object(objectName)
                .expiry(duration, unit)
                .build();

        try {
            return minioClient.getPresignedObjectUrl(objectUrlArgs);
        } catch (Exception e) {
            logger.error("getObjectUrl {}", e.getMessage());
        }

        return null;
    }

    /**
     * 获得对象url
     *
     * @param objectName 对象名称
     * @param duration   超时时长
     * @param unit       单位
     * @return {@link String} 对象url
     */
    public String getObjectUrl(String objectName, int duration, TimeUnit unit) {
        return this.getObjectUrl(minioProperties.getBucketName(), objectName, duration, unit);
    }

    /**
     * 获得对象url
     *
     * @param objectName 对象名称
     * @return {@link String} 对象url
     */
    public String getObjectUrl(String objectName) {
        return this.getObjectUrl(objectName, 30, TimeUnit.MINUTES);
    }

    /**
     * 获得对象url
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link String} 对象url
     */
    public String getObjectUrl(String bucketName, String objectName) {
        return this.getObjectUrl(bucketName, objectName, 30, TimeUnit.MINUTES);
    }

    /**
     * 删除对象
     *
     * @param bucketName bucket名称
     * @param versionId  版本ID
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean removeObject(String bucketName, String objectName, String versionId) {

        RemoveObjectArgs args;
        if (StrUtil.isBlank(versionId)) {
            args = RemoveObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .build();
        } else {
            args = RemoveObjectArgs.builder()
                    .bucket(bucketName)
                    .versionId(versionId)
                    .object(objectName)
                    .build();
        }

        try {
            minioClient.removeObject(args);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("removeObject {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除对象
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean removeObject(String bucketName, String objectName) {
        return removeObject(bucketName, objectName, null);
    }

    /**
     * 删除对象
     *
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean removeObject(String objectName) {
        return removeObject(minioProperties.getBucketName(), objectName);
    }

    /**
     * 删除对象
     *
     * @param removeObjects 删除对象集合
     * @return {@link List<RemoveObject>}  失败列表
     */
    public List<RemoveObject> removeObject(List<RemoveObject> removeObjects) {

        removeObjects.stream().filter(a -> StrUtil.isBlank(a.getBucketName()))
                .forEach(a -> a.setBucketName(minioProperties.getBucketName()));

        Map<String, List<RemoveObject>> removeObjectMap = removeObjects.stream()
                .collect(Collectors.groupingBy(RemoveObject::getBucketName));

        return removeObjectMap.entrySet().stream().map(object -> {

            String bucketName = object.getKey();
            List<RemoveObject> removeObjectList = object.getValue();

            List<DeleteObject> objects = new LinkedList<>();
            for (RemoveObject removeObject : removeObjectList) {
                DeleteObject deleteObject = StrUtil.isBlank(removeObject.getVersionId())
                        ? new DeleteObject(removeObject.getObjectName())
                        : new DeleteObject(removeObject.getObjectName(), removeObject.getVersionId());
                objects.add(deleteObject);
            }

            RemoveObjectsArgs removeObjectsArgs = RemoveObjectsArgs.builder()
                    .objects(objects)
                    .bucket(bucketName)
                    .build();

            Iterable<Result<DeleteError>> removeObjectsResults = minioClient.removeObjects(removeObjectsArgs);

            RemoveObject removeObject = null;
            for (Result<DeleteError> removeObjectsResult : removeObjectsResults) {
                try {
                    DeleteError deleteError = removeObjectsResult.get();
                    logger.error("Error in deleting object " + deleteError.objectName() + "; " + deleteError.message());
                    removeObject = new RemoveObject(deleteError.bucketName(), deleteError.objectName());
                } catch (Exception e) {
                    logger.error("removeObject {}", e.getMessage());
                }
            }
            return removeObject;
        }).filter(ObjectUtil::isNotNull).collect(Collectors.toList());
    }

    /**
     * 列表对象信息
     *
     * @param listObjectParam 列表对象参数
     * @return {@link List<Item>} 对象信息
     * @throws MinioException minio异常
     */
    public List<Item> listObjects(ListObjectParam listObjectParam) throws MinioException {

        String bucketName = StrUtil.isBlank(listObjectParam.getBucketName())
                ? minioProperties.getBucketName() : listObjectParam.getBucketName();

        Integer maxKeys = listObjectParam.getMaxKeys();
        String prefix = listObjectParam.getPrefix();
        String startAfter = listObjectParam.getStartAfter();
        boolean includeVersions = listObjectParam.isIncludeVersions();

        ListObjectsArgs.Builder builder = ListObjectsArgs.builder();
        builder.bucket(bucketName).includeVersions(includeVersions).maxKeys(maxKeys);
        if (StrUtil.isNotBlank(startAfter)) {
            builder.startAfter(startAfter);
        }

        if (StrUtil.isNotBlank(prefix)) {
            builder.prefix(prefix);
        }

        ListObjectsArgs objectsArgs = builder.build();

        Iterable<Result<Item>> listObjects = minioClient.listObjects(objectsArgs);
        return MinioUtils.getItems(listObjects);
    }

    /**
     * 获取bucket集合
     *
     * @return {@link List<Bucket>}
     * @throws MinioException minio异常
     */
    public List<Bucket> listBuckets() throws MinioException {

        try {
            return minioClient.listBuckets();
        } catch (Exception e) {
            logger.error("listBuckets {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 创建 bucket
     *
     * @param bucketName bucket名称
     * @param objectLock 对象锁
     * @return {@link Boolean} 是否成功
     */
    public Boolean makeBucket(String bucketName, boolean objectLock) {

        MakeBucketArgs makeBucketArgs = MakeBucketArgs.builder()
                .bucket(bucketName)
                .objectLock(objectLock)
                .build();

        try {
            minioClient.makeBucket(makeBucketArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("makeBucket {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 创建 bucket
     *
     * @param bucketName bucket名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean makeBucket(String bucketName) {
        return this.makeBucket(bucketName, Boolean.FALSE);
    }

    /**
     * bucket 版本控制
     *
     * @param bucketName          bucket名称
     * @param bucketVersionStatus 桶版本状态
     * @param mfaDelete           mfa删除
     * @return {@link Boolean}
     */
    public Boolean setBucketVersion(String bucketName,
                                    BucketVersionStatus bucketVersionStatus,
                                    Boolean mfaDelete) {

        VersioningConfiguration versioning = new VersioningConfiguration(VersioningConfiguration.Status.fromString(bucketVersionStatus.getValue()),
                mfaDelete);

        SetBucketVersioningArgs bucketVersioningArgs = SetBucketVersioningArgs.builder()
                .bucket(bucketName)
                .config(versioning)
                .build();

        try {
            minioClient.setBucketVersioning(bucketVersioningArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("setBucketVersion {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 获取bucket版本
     *
     * @param bucketName bucket名称
     * @return {@link VersioningConfiguration} 版本信息
     * @throws MinioException minio异常
     */
    public VersioningConfiguration getBucketVersion(String bucketName) throws MinioException {

        GetBucketVersioningArgs bucketVersioningArgs = GetBucketVersioningArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            return minioClient.getBucketVersioning(bucketVersioningArgs);
        } catch (Exception e) {
            logger.error("getBucketVersion {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 设置对象锁配置
     *
     * @param bucketName    bucket名称
     * @param retentionMode 保留模式
     * @param duration      持续时间
     * @param isDays        是天？
     * @return {@link Boolean}
     */
    public Boolean setObjectLockConfiguration(String bucketName,
                                              RetentionMode retentionMode,
                                              int duration, boolean isDays) {

        RetentionDuration retentionDuration =
                isDays ? new RetentionDurationDays(duration)
                        : new RetentionDurationYears(duration);

        ObjectLockConfiguration objectLockConfiguration
                = new ObjectLockConfiguration(retentionMode, retentionDuration);

        SetObjectLockConfigurationArgs lockConfigurationArgs = SetObjectLockConfigurationArgs.builder()
                .bucket(bucketName)
                .config(objectLockConfiguration)
                .build();

        try {
            minioClient.setObjectLockConfiguration(lockConfigurationArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("setObjectLockConfiguration {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除对象锁配置
     *
     * @param bucketName bucket名称
     * @return {@link Boolean} 是否删除成功
     */
    public Boolean deleteObjectLockConfiguration(String bucketName) {

        DeleteObjectLockConfigurationArgs lockConfigurationArgs = DeleteObjectLockConfigurationArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            minioClient.deleteObjectLockConfiguration(lockConfigurationArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("deleteObjectLockConfiguration {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 获取对象锁配置
     *
     * @param bucketName bucket名称
     * @return {@link ObjectLockConfiguration} 配置信息
     * @throws MinioException minio异常
     */
    public ObjectLockConfiguration getObjectLockConfiguration(String bucketName) throws MinioException {

        GetObjectLockConfigurationArgs lockConfigurationArgs = GetObjectLockConfigurationArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            return minioClient.getObjectLockConfiguration(lockConfigurationArgs);
        } catch (Exception e) {
            logger.error("getObjectLockConfiguration {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 对象保留
     *
     * @param bucketName           bucket名称
     * @param objectName           对象名称
     * @param retentionMode        保留模式
     * @param zonedDateTime        划定的日期时间
     * @param bypassGovernanceMode 绕过治理模式
     * @return {@link Boolean}
     */
    public Boolean setObjectRetention(String bucketName, String objectName,
                                      RetentionMode retentionMode,
                                      ZonedDateTime zonedDateTime,
                                      boolean bypassGovernanceMode) {

        Retention retention = new Retention(retentionMode, zonedDateTime);

        SetObjectRetentionArgs setObjectRetentionArgs = SetObjectRetentionArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .config(retention)
                .bypassGovernanceMode(bypassGovernanceMode)
                .build();

        try {
            minioClient.setObjectRetention(setObjectRetentionArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("setObjectRetention {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 对象保留
     *
     * @param objectName           对象名称
     * @param retentionMode        保留模式
     * @param zonedDateTime        划定的日期时间
     * @param bypassGovernanceMode 绕过治理模式
     * @return {@link Boolean}
     */
    public Boolean setObjectRetention(String objectName,
                                      RetentionMode retentionMode,
                                      ZonedDateTime zonedDateTime,
                                      boolean bypassGovernanceMode) {
        return this.setObjectRetention(minioProperties.getBucketName(), objectName,
                retentionMode, zonedDateTime, bypassGovernanceMode);
    }

    /**
     * 获取对象保留
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link Retention} 保留信息
     * @throws MinioException minio异常
     */
    public Retention getObjectRetention(String bucketName, String objectName) throws MinioException {

        GetObjectRetentionArgs objectRetentionArgs = GetObjectRetentionArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();

        try {
            return minioClient.getObjectRetention(objectRetentionArgs);
        } catch (Exception e) {
            logger.error("getObjectRetention {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 获取对象保留
     *
     * @param objectName 对象名称
     * @return {@link Retention} 保留信息
     * @throws MinioException minio异常
     */
    public Retention getObjectRetention(String objectName) throws MinioException {
        return this.getObjectRetention(minioProperties.getBucketName(), objectName);
    }

    /**
     * 开启对象合法持有
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean enableObjectLegalHold(String bucketName, String objectName) {

        EnableObjectLegalHoldArgs objectLegalHoldArgs = EnableObjectLegalHoldArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();

        try {
            minioClient.enableObjectLegalHold(objectLegalHoldArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("enableObjectLegalHold {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 开启对象合法持有
     *
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean enableObjectLegalHold(String objectName) {
        return this.enableObjectLegalHold(minioProperties.getBucketName(), objectName);
    }

    /**
     * 关闭对象合法持有
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean disableObjectLegalHold(String bucketName, String objectName) {

        DisableObjectLegalHoldArgs objectLegalHoldArgs = DisableObjectLegalHoldArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();

        try {
            minioClient.disableObjectLegalHold(objectLegalHoldArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("disableObjectLegalHold {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 关闭对象合法持有
     *
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean disableObjectLegalHold(String objectName) {
        return this.disableObjectLegalHold(minioProperties.getBucketName(), objectName);
    }

    /**
     * 是否开启对象合法持有
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean isObjectLegalHoldEnabled(String bucketName, String objectName) {

        IsObjectLegalHoldEnabledArgs objectLegalHoldArgs = IsObjectLegalHoldEnabledArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();

        try {
            return minioClient.isObjectLegalHoldEnabled(objectLegalHoldArgs);
        } catch (Exception e) {
            logger.error("isObjectLegalHoldEnabled {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 是否开启对象合法持有
     *
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean isObjectLegalHoldEnabled(String objectName) {
        return this.isObjectLegalHoldEnabled(minioProperties.getBucketName(), objectName);
    }

    /**
     * 删除bucket
     *
     * @param bucketName bucket名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean removeBucket(String bucketName) {

        if (!bucketExists(bucketName)) {
            return Boolean.TRUE;
        }

        RemoveBucketArgs removeBucketArgs = RemoveBucketArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            minioClient.removeBucket(removeBucketArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("removeBucket {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 上传对象
     *
     * @param bucketName  bucket名称
     * @param objectName  对象名称
     * @param file        文件
     * @param contentType 内容类型
     * @throws MinioException minio异常
     */
    public void putObject(String bucketName, String objectName,
                          InputStream file, String contentType) throws MinioException {

        try {
            PutObjectArgs.Builder builder = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(file, file.available(), -1)
                    .contentType(contentType);

            if (StrUtil.isNotBlank(contentType)) {
                builder.contentType(contentType);
            }

            minioClient.putObject(builder.build());
        } catch (Exception e) {
            logger.error("putObject {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 上传对象
     *
     * @param objectName  对象名称
     * @param file        文件
     * @param contentType 内容类型
     * @throws MinioException minio异常
     */
    public void putObject(String objectName,
                          InputStream file, String contentType) throws MinioException {
        this.putObject(minioProperties.getBucketName(), objectName, file, contentType);
    }

    /**
     * 上传对象
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @param file       文件
     * @throws MinioException minio异常
     */
    public void putObject(String bucketName, String objectName,
                          InputStream file) throws MinioException {
        this.putObject(bucketName, objectName, file, null);
    }

    /**
     * 上传对象
     *
     * @param objectName 对象名称
     * @param file       文件
     * @throws MinioException minio异常
     */
    public void putObject(String objectName, InputStream file) throws MinioException {
        this.putObject(minioProperties.getBucketName(), objectName, file);
    }

    /**
     * 上传对象
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @param file       文件
     * @throws MinioException minio异常
     */
    public void putObject(String bucketName, String objectName, byte[] file) throws MinioException {
        this.putObject(bucketName, objectName, new ByteArrayInputStream(file));
    }

    /**
     * 上传对象
     *
     * @param objectName 对象名称
     * @param file       文件
     * @throws MinioException minio异常
     */
    public void putObject(String objectName, byte[] file) throws MinioException {
        this.putObject(minioProperties.getBucketName(), objectName, file);
    }

    /**
     * 上传对象
     *
     * @param bucketName  bucket名称
     * @param objectName  对象名称
     * @param file        文件
     * @param contentType 内容类型
     * @throws MinioException minio异常
     */
    public void putObject(String bucketName, String objectName, File file, String contentType) throws MinioException {

        try {

            UploadObjectArgs.Builder builder = UploadObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .filename(file.getAbsolutePath());

            if (StrUtil.isNotBlank(contentType)) {
                builder.contentType(contentType);
            }

            minioClient.uploadObject(builder.build());
        } catch (Exception e) {
            logger.error("putObject {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }

    }

    /**
     * 上传对象
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @param file       文件
     * @throws MinioException minio异常
     */
    public void putObject(String bucketName, String objectName, File file) throws MinioException {
        this.putObject(bucketName, objectName, file, null);
    }

    /**
     * 上传对象
     *
     * @param objectName  对象名称
     * @param contentType 内容类型
     * @param file        文件
     * @throws MinioException minio异常
     */
    public void putObject(String objectName, File file, String contentType) throws MinioException {
        this.putObject(minioProperties.getBucketName(), objectName, file, contentType);
    }

    /**
     * 上传对象
     *
     * @param objectName 对象名称
     * @param file       文件
     * @throws MinioException minio异常
     */
    public void putObject(String objectName, File file) throws MinioException {
        this.putObject(minioProperties.getBucketName(), objectName, file);
    }

    /**
     * 上传对象
     *
     * @param file 文件
     * @throws MinioException minio异常
     */
    public void putObject(File file) throws MinioException {
        this.putObject(file.getName(), file);
    }

    /**
     * 获取bucket策略
     *
     * @param bucketName bucket名称
     * @return {@link String} 策略
     * @throws MinioException minio异常
     */
    public String getBucketPolicy(String bucketName) throws MinioException {

        GetBucketPolicyArgs bucketPolicyArgs = GetBucketPolicyArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            return minioClient.getBucketPolicy(bucketPolicyArgs);
        } catch (Exception e) {
            logger.error("getBucketPolicy {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 制定桶策略
     *
     * @param bucketName        bucket名称
     * @param bucketPolicyParam 桶策略参数
     * @return {@link Boolean}
     */
    public Boolean setBucketPolicy(String bucketName, BucketPolicyParam bucketPolicyParam) {

        SetBucketPolicyArgs bucketPolicyArgs = SetBucketPolicyArgs.builder().bucket(bucketName)
                .config(JSON.toJSONString(bucketPolicyParam)).build();

        try {
            minioClient.setBucketPolicy(bucketPolicyArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("setBucketPolicy {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除bucket 策略
     *
     * @param bucketName bucket名称
     * @return {@link Boolean}
     */
    public Boolean deleteBucketPolicy(String bucketName) {

        DeleteBucketPolicyArgs bucketPolicyArgs = DeleteBucketPolicyArgs.builder()
                .bucket(bucketName).build();

        try {
            minioClient.deleteBucketPolicy(bucketPolicyArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("deleteBucketPolicy {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 设置 bucketName生命周期
     *
     * @param bucketName bucket名称
     * @param rules      规则
     * @return {@link Boolean} 是否成功
     */
    public Boolean setBucketLifecycle(String bucketName, List<LifecycleRule> rules) {

        LifecycleConfiguration config = new LifecycleConfiguration(rules);

        try {
            minioClient.setBucketLifecycle(SetBucketLifecycleArgs.builder().bucket(bucketName).config(config).build());
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("setBucketLifecycle {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除bucket生命周期
     *
     * @param bucketName bucket名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean deleteBucketLifecycle(String bucketName) {

        try {
            minioClient.deleteBucketLifecycle(DeleteBucketLifecycleArgs.builder().bucket(bucketName).build());
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("deleteBucketLifecycle {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 获取桶生命周期
     *
     * @param bucketName bucket名称
     * @return {@link LifecycleConfiguration}
     * @throws MinioException minio异常
     */
    public LifecycleConfiguration getBucketLifecycle(String bucketName) throws MinioException {

        try {
            return minioClient.getBucketLifecycle(GetBucketLifecycleArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            logger.error("getBucketLifecycle {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 获取桶通知
     *
     * @param bucketName bucket名称
     * @return {@link NotificationConfiguration}
     * @throws MinioException minio异常
     */
    public NotificationConfiguration getBucketNotification(String bucketName) throws MinioException {

        GetBucketNotificationArgs notificationArgs = GetBucketNotificationArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            return minioClient.getBucketNotification(notificationArgs);
        } catch (Exception e) {
            logger.error("getBucketNotification {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 设置水桶通知
     *
     * @param bucketName          bucket名称
     * @param queueConfigurations 队列配置
     * @return {@link Boolean}
     */
    public Boolean setBucketNotification(String bucketName, List<QueueConfiguration> queueConfigurations) {

        NotificationConfiguration config = new NotificationConfiguration();
        config.setQueueConfigurationList(queueConfigurations);

        SetBucketNotificationArgs bucketNotificationArgs = SetBucketNotificationArgs.builder().bucket(bucketName)
                .config(config).build();

        try {
            minioClient.setBucketNotification(bucketNotificationArgs);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("setBucketNotification {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除桶通知
     *
     * @param bucketName bucket名称
     * @return {@link Boolean}
     */
    public Boolean deleteBucketNotification(String bucketName) {

        DeleteBucketNotificationArgs bucketNotificationArgs = DeleteBucketNotificationArgs.builder()
                .bucket(bucketName).build();

        try {

            minioClient.deleteBucketNotification(bucketNotificationArgs);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("deleteBucketNotification {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 获取桶复制配置
     *
     * @param bucketName bucket名称
     * @return {@link ReplicationConfiguration}
     * @throws MinioException minio异常
     */
    public ReplicationConfiguration getBucketReplication(String bucketName) throws MinioException {

        GetBucketReplicationArgs bucketReplicationArgs = GetBucketReplicationArgs.builder().bucket(bucketName).build();

        try {

            return minioClient.getBucketReplication(bucketReplicationArgs);
        }catch (Exception e) {
            logger.error("deleteBucketNotification {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }

    }

    /**
     * 设置水桶复制
     *
     * @param bucketName bucket名称
     * @param rules      规则
     * @return {@link Boolean}
     */
    public Boolean setBucketReplication(String bucketName, List<ReplicationRule> rules) {

        ReplicationConfiguration config = new ReplicationConfiguration("REPLACE-WITH-ACTUAL-ROLE", rules);

        SetBucketReplicationArgs bucketReplicationArgs = SetBucketReplicationArgs.builder().bucket(bucketName).config(config).build();

        try {
            minioClient.setBucketReplication(bucketReplicationArgs);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("setBucketReplication {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除桶复制
     *
     * @param bucketName bucket名称
     * @return {@link Boolean}
     */
    public Boolean deleteBucketReplication(String bucketName) {

        try {
            minioClient.deleteBucketReplication(DeleteBucketReplicationArgs.builder().bucket(bucketName).build());
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("deleteBucketReplication {}", e.getMessage());
        }

        return Boolean.FALSE;
    }





















}