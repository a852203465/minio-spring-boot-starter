package cn.darkjrong.minio;

import cn.darkjrong.minio.domain.BucketPolicyParam;
import cn.darkjrong.minio.domain.ListObjectParam;
import cn.darkjrong.minio.domain.RemoveObject;
import cn.darkjrong.minio.enums.BucketVersionStatus;
import cn.darkjrong.minio.enums.ExceptionEnum;
import cn.darkjrong.minio.exceptions.MinioException;
import cn.darkjrong.spring.boot.autoconfigure.MinioProperties;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.system.SystemUtil;
import com.alibaba.fastjson.JSON;
import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    private static final Integer DURATION = 30;
    private static final Integer ZERO = 0;

    private final MinioClient minioClient;
    private final MinioProperties minioProperties;

    public MinioTemplate(MinioClient minioClient, MinioProperties minioProperties) {
        this.minioClient = minioClient;
        this.minioProperties = minioProperties;
    }

    /**
     * 获取minio客户端
     *
     * @return {@link MinioClient}
     */
    public MinioClient getMinioClient() {
        return minioClient;
    }

    /**
     * 判断bucket是否存在
     *
     * @param bucketName bucket名称
     * @return {@link Boolean} 是否存在
     */
    public Boolean bucketExists(String bucketName) {

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder()
                .bucket(bucketName)
                .build();
        try {
            return minioClient.bucketExists(bucketExistsArgs);
        } catch (Exception e) {
            logger.error("判断bucket是否存在异常 {}", e.getMessage());
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
        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
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
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
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

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);

        StatObjectArgs.Builder builder = StatObjectArgs.builder().bucket(bucketName).object(objectName);
        if (StrUtil.isNotBlank(versionId)) {
            builder.versionId(versionId);
        }

        try {
            return minioClient.statObject(builder.build());
        } catch (Exception e) {
            logger.error("获取对象信息异常 {}", e.getMessage());
            throw new MinioException("获取对象信息异常", e);
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
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
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

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);

        GetObjectArgs getObjectArgs = GetObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();

        try {
            return IoUtil.readBytes(minioClient.getObject(getObjectArgs));
        } catch (Exception e) {
            logger.error("获取对象异常 {}", e.getMessage());
            throw new MinioException("获取对象异常", e);
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

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(fileName, ExceptionEnum.FILE_NAME_CANNOT_BE_EMPTY);

        DownloadObjectArgs args = DownloadObjectArgs.builder()
                .filename(fileName)
                .object(objectName)
                .bucket(bucketName)
                .build();

        try {
            minioClient.downloadObject(args);
        } catch (Exception e) {
            logger.error("下载对象异常 {}", e.getMessage());
            throw new MinioException("下载对象异常", e);
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

        MinioUtils.notEmpty(fileName, ExceptionEnum.FILE_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);

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
     * @return {@link String} 目标对象名
     * @throws MinioException minio异常
     */
    public String copyObject(String srcBucketName, String targetBucketName, String objectName) throws MinioException {
        return copyObject(srcBucketName, targetBucketName, objectName, objectName);
    }

    /**
     * 复制对象
     *
     * <p>
     * 在bucket中复制一份
     * </p>
     *
     * @param bucketName    bucket名称
     * @param objectName       对象名称
     * @return {@link String} 目标对象名
     * @throws MinioException minio异常
     */
    public String copyObject(String bucketName, String objectName) throws MinioException {
        String target = StrUtil.sub(objectName, ZERO , StrUtil.lastIndexOfIgnoreCase(objectName, StrUtil.SLASH)) + StrUtil.DOT + FileUtil.extName(objectName);
        return copyObject(bucketName, bucketName, objectName, target);
    }

    /**
     * 复制对象
     *
     * <p>
     * 在bucket中复制一份
     * </p>
     *
     * @param objectName       对象名称
     * @return {@link String} 目标对象名
     * @throws MinioException minio异常
     */
    public String copyObject(String objectName) throws MinioException {
        String target = StrUtil.sub(objectName, ZERO , StrUtil.lastIndexOfIgnoreCase(objectName, StrUtil.SLASH)) +
                StrUtil.SLASH + IdUtil.fastSimpleUUID() + DateUtil.current() + StrUtil.DOT + FileUtil.extName(objectName);
        return copyObject(minioProperties.getBucketName(), minioProperties.getBucketName(), objectName, target);
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
     * @param targetObjectName 目标对象的名字
     * @return {@link String} 目标对象名
     * @throws MinioException minio异常
     */
    public String copyObject(String srcBucketName, String targetBucketName,
                           String srcObjectName, String targetObjectName) throws MinioException {

        MinioUtils.notEmpty(srcBucketName, ExceptionEnum.SOURCE_BUCKET_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(targetBucketName, ExceptionEnum.TARGET_BUCKET_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(srcObjectName, ExceptionEnum.SOURCE_OBJECT_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(targetObjectName, ExceptionEnum.TARGET_OBJECT_CANNOT_BE_EMPTY);

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
            return minioClient.copyObject(copyObjectArgs).object();
        } catch (Exception e) {
            logger.error("复制对象异常 {}", e.getMessage());
            throw new MinioException("复制对象异常", e);
        }
    }

    /**
     * 获得对象url
     *
     * @param duration   超时时长，默认：30
     * @param unit       单位, 默认：分钟
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link String} 对象url
     * @throws MinioException minio异常
     */
    public String getObjectUrl(String bucketName, String objectName, int duration, TimeUnit unit) throws MinioException {

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
        if (ObjectUtil.isNull(duration) || duration <= ZERO) duration = DURATION;
        if (ObjectUtil.isNull(unit)) unit = TimeUnit.MINUTES;

        GetPresignedObjectUrlArgs objectUrlArgs = GetPresignedObjectUrlArgs.builder()
                .method(Method.GET)
                .bucket(bucketName)
                .object(objectName)
                .expiry(duration, unit)
                .build();

        try {
            return minioClient.getPresignedObjectUrl(objectUrlArgs);
        } catch (Exception e) {
            logger.error("获取对象URL异常 {}", e.getMessage());
            throw new MinioException("获取对象URL异常", e);
        }
    }

    /**
     * 获得对象url
     *
     * @param objectName 对象名称
     * @param duration   超时时长
     * @param unit       单位
     * @return {@link String} 对象url
     * @throws MinioException minio异常
     */
    public String getObjectUrl(String objectName, int duration, TimeUnit unit) throws MinioException {
        return this.getObjectUrl(minioProperties.getBucketName(), objectName, duration, unit);
    }

    /**
     * 获得对象url
     *
     * @param objectName 对象名称
     * @return {@link String} 对象url
     * @throws MinioException minio异常
     */
    public String getObjectUrl(String objectName) throws MinioException {
        return this.getObjectUrl(objectName, DURATION, TimeUnit.MINUTES);
    }

    /**
     * 获得对象url
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link String} 对象url
     * @throws MinioException minio异常
     */
    public String getObjectUrl(String bucketName, String objectName) throws MinioException {
        return this.getObjectUrl(bucketName, objectName, DURATION, TimeUnit.MINUTES);
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

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);

        RemoveObjectArgs.Builder builder = RemoveObjectArgs.builder().bucket(bucketName).object(objectName);
        if (StrUtil.isNotBlank(versionId)) {
            builder.versionId(versionId);
        }

        try {
            minioClient.removeObject(builder.build());
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("删除对象异常 {}", e.getMessage());
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

        MinioUtils.notEmpty(removeObjects, ExceptionEnum.THE_OBJECT_COLLECTION_CANNOT_BE_EMPTY);

        removeObjects.stream().filter(a -> StrUtil.isBlank(a.getBucketName()))
                .forEach(a -> a.setBucketName(minioProperties.getBucketName()));

        // key: bucket, value: 删除集合
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
                    logger.error("删除对象异常 {}", e.getMessage());
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

        String bucketName = StrUtil.isBlank(listObjectParam.getBucketName()) ? minioProperties.getBucketName() : listObjectParam.getBucketName();

        Integer maxKeys = listObjectParam.getMaxKeys();
        String prefix = listObjectParam.getPrefix();
        String startAfter = listObjectParam.getStartAfter();
        boolean includeVersions = listObjectParam.isIncludeVersions();

        ListObjectsArgs.Builder builder = ListObjectsArgs.builder();
        builder.bucket(bucketName).includeVersions(includeVersions).maxKeys(maxKeys);
        if (StrUtil.isNotBlank(startAfter)) builder.startAfter(startAfter);
        if (StrUtil.isNotBlank(prefix)) builder.prefix(prefix);
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
            logger.error("获取bucket集合异常 {}", e.getMessage());
            throw new MinioException("获取bucket集合异常", e);
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

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);

        MakeBucketArgs makeBucketArgs = MakeBucketArgs.builder()
                .bucket(bucketName)
                .objectLock(objectLock)
                .build();

        try {
            minioClient.makeBucket(makeBucketArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("创建 bucket 异常 {}", e.getMessage());
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
     * @param bucketVersionStatus bucket版本状态
     * @param mfaDelete           mfa删除
     * @return {@link Boolean}
     */
    public Boolean setBucketVersion(String bucketName,
                                    BucketVersionStatus bucketVersionStatus,
                                    boolean mfaDelete) {

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        if(ObjectUtil.isNull(bucketVersionStatus)) bucketVersionStatus = BucketVersionStatus.ENABLED;
        VersioningConfiguration versioning = new VersioningConfiguration(VersioningConfiguration.Status.fromString(bucketVersionStatus.getValue()), mfaDelete);

        SetBucketVersioningArgs bucketVersioningArgs = SetBucketVersioningArgs.builder()
                .bucket(bucketName)
                .config(versioning)
                .build();

        try {
            minioClient.setBucketVersioning(bucketVersioningArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("设置bucket版本异常 {}", e.getMessage());
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

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);

        GetBucketVersioningArgs bucketVersioningArgs = GetBucketVersioningArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            return minioClient.getBucketVersioning(bucketVersioningArgs);
        } catch (Exception e) {
            logger.error("获取bucket版本异常 {}", e.getMessage());
            throw new MinioException("获取bucket版本异常", e);
        }
    }

    /**
     * 设置对象锁配置
     *
     * @param bucketName    bucket名称
     * @param retentionMode 保留模式
     * @param duration      持续时间, 默认：30
     * @param isDays        是天？
     * @return {@link Boolean}
     */
    public Boolean setObjectLockConfiguration(String bucketName, RetentionMode retentionMode, int duration, boolean isDays) {

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);

        if (duration <= ZERO) duration = DURATION;
        if (ObjectUtil.isNull(retentionMode)) retentionMode = RetentionMode.COMPLIANCE;
        RetentionDuration retentionDuration = isDays ? new RetentionDurationDays(duration) : new RetentionDurationYears(duration);
        ObjectLockConfiguration objectLockConfiguration = new ObjectLockConfiguration(retentionMode, retentionDuration);

        SetObjectLockConfigurationArgs lockConfigurationArgs = SetObjectLockConfigurationArgs.builder()
                .bucket(bucketName)
                .config(objectLockConfiguration)
                .build();

        try {
            minioClient.setObjectLockConfiguration(lockConfigurationArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("设置对象锁配置异常 {}", e.getMessage());
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

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);

        DeleteObjectLockConfigurationArgs lockConfigurationArgs = DeleteObjectLockConfigurationArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            minioClient.deleteObjectLockConfiguration(lockConfigurationArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("删除对象锁配置异常 {}", e.getMessage());
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

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);

        GetObjectLockConfigurationArgs lockConfigurationArgs = GetObjectLockConfigurationArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            return minioClient.getObjectLockConfiguration(lockConfigurationArgs);
        } catch (Exception e) {
            logger.error("获取对象锁配置异常 {}", e.getMessage());
            throw new MinioException("获取对象锁配置异常", e);
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
    public Boolean setObjectRetention(String bucketName, String objectName, RetentionMode retentionMode, ZonedDateTime zonedDateTime, boolean bypassGovernanceMode) {

        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);

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
            logger.error("设置对象保留时间异常 {}", e.getMessage());
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
    public Boolean setObjectRetention(String objectName, RetentionMode retentionMode, ZonedDateTime zonedDateTime, boolean bypassGovernanceMode) {
        return this.setObjectRetention(minioProperties.getBucketName(), objectName, retentionMode, zonedDateTime, bypassGovernanceMode);
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
        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
        GetObjectRetentionArgs objectRetentionArgs = GetObjectRetentionArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();

        try {
            return minioClient.getObjectRetention(objectRetentionArgs);
        } catch (Exception e) {
            logger.error("获取对象保留信息异常 {}", e.getMessage());
            throw new MinioException("获取对象保留信息异常", e);
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
        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
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
        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
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
        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
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
        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        if (!bucketExists(bucketName)) return Boolean.TRUE;

        RemoveBucketArgs removeBucketArgs = RemoveBucketArgs.builder().bucket(bucketName).build();

        try {
            minioClient.removeBucket(removeBucketArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("删除bucket异常 {}", e.getMessage());
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
     * @return {@link String} 上传对象名
     * @throws MinioException minio异常
     */
    public String putObject(String bucketName, String objectName, InputStream file, String contentType) throws MinioException {
        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(file, ExceptionEnum.FILE_NAME_CANNOT_BE_EMPTY);
        objectName = MinioUtils.getDateFolder() + StrUtil.SLASH + objectName;
        try {
            PutObjectArgs.Builder builder = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(file, file.available(), -1);

            if (StrUtil.isNotBlank(contentType)) builder.contentType(contentType);
            return minioClient.putObject(builder.build()).object();
        } catch (Exception e) {
            logger.error("对象 : {} 上传异常 {}", objectName, e.getMessage());
            throw new MinioException("上传对象异常", e);
        }finally {
            IoUtil.close(file);
        }
    }

    /**
     * 上传对象
     *
     * @param objectName  对象名称
     * @param file        文件
     * @param contentType 内容类型
     * @return {@link String} 上传对象名
     * @throws MinioException minio异常
     */
    public String putObject(String objectName, InputStream file, String contentType) throws MinioException {
        return this.putObject(minioProperties.getBucketName(), objectName, file, contentType);
    }

    /**
     * 上传对象
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @param file       文件
     * @return {@link String} 上传对象名
     * @throws MinioException minio异常
     */
    public String putObject(String bucketName, String objectName, InputStream file) throws MinioException {
        return this.putObject(bucketName, objectName, file, null);
    }

    /**
     * 上传对象
     *
     * @param objectName 对象名称
     * @param file       文件
     * @return {@link String} 上传对象名
     * @throws MinioException minio异常
     */
    public String putObject(String objectName, InputStream file) throws MinioException {
        return this.putObject(minioProperties.getBucketName(), objectName, file);
    }

    /**
     * 上传对象
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @param file       文件
     * @return {@link String} 上传对象名
     * @throws MinioException minio异常
     */
    public String putObject(String bucketName, String objectName, MultipartFile file) throws MinioException {
        Assert.isFalse(file.isEmpty(), ExceptionEnum.FILE_NAME_CANNOT_BE_EMPTY.getValue());
        try {
            return this.putObject(bucketName, objectName, file.getInputStream(), null);
        }catch (IOException e) {
            logger.error("上传对象异常 {}", e.getMessage());
            throw new MinioException("上传对象异常", e);
        }
    }

    /**
     * 上传对象
     *
     * @param objectName 对象名称
     * @param file       文件
     * @return {@link String} 上传对象名
     * @throws MinioException minio异常
     */
    public String putObject(String objectName, MultipartFile file) throws MinioException {
        return this.putObject(minioProperties.getBucketName(), objectName, file);
    }

    /**
     * 上传对象
     *
     * @param file       文件
     * @return {@link String} 上传对象名
     * @throws MinioException minio异常
     */
    public String putObject(MultipartFile file) throws MinioException {
        Assert.isFalse(file.isEmpty(), ExceptionEnum.FILE_NAME_CANNOT_BE_EMPTY.getValue());
        String extName = FileUtil.extName(file.getOriginalFilename());
        Assert.notBlank(extName, "非法文件名称：" + file.getOriginalFilename());
        return this.putObject(minioProperties.getBucketName(), IdUtil.fastSimpleUUID() + DateUtil.current() + StrUtil.DOT + extName, file);
    }

    /**
     * 上传对象
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @param file       文件
     * @return {@link String} 上传对象名
     * @throws MinioException minio异常
     */
    public String putObject(String bucketName, String objectName, byte[] file) throws MinioException {
        return this.putObject(bucketName, objectName, new ByteArrayInputStream(file));
    }

    /**
     * 上传对象
     *
     * @param objectName 对象名称
     * @param file       文件
     * @return {@link String} 上传对象名
     * @throws MinioException minio异常
     */
    public String putObject(String objectName, byte[] file) throws MinioException {
        return this.putObject(minioProperties.getBucketName(), objectName, file);
    }

    /**
     * 上传对象
     *
     * @param bucketName  bucket名称
     * @param objectName  对象名称
     * @param file        文件
     * @param contentType 内容类型
     * @return {@link String} 文件名
     * @throws MinioException minio异常
     */
    public String putObject(String bucketName, String objectName, File file, String contentType) throws MinioException {
        MinioUtils.notEmpty(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY);
        MinioUtils.notEmpty(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY);
        Assert.isTrue(FileUtil.exist(file), ExceptionEnum.FILE_NAME_CANNOT_BE_EMPTY.getValue());
        objectName = MinioUtils.getDateFolder() + StrUtil.SLASH + objectName;
        try {

            UploadObjectArgs.Builder builder = UploadObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .filename(file.getAbsolutePath());
            if (StrUtil.isNotBlank(contentType)) builder.contentType(contentType);

            return minioClient.uploadObject(builder.build()).object();
        } catch (Exception e) {
            logger.error("文件 : {} 上传异常,  {}", objectName, e.getMessage());
            throw new MinioException("文件上传异常", e);
        }
    }

    /**
     * 上传对象
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @param file       文件
     * @return {@link String} 文件名
     * @throws MinioException minio异常
     */
    public String putObject(String bucketName, String objectName, File file) throws MinioException {
        return this.putObject(bucketName, objectName, file, null);
    }

    /**
     * 上传对象
     *
     * @param objectName  对象名称
     * @param contentType 内容类型
     * @param file        文件
     * @return {@link String} 文件名
     * @throws MinioException minio异常
     */
    public String putObject(String objectName, File file, String contentType) throws MinioException {
        return this.putObject(minioProperties.getBucketName(), objectName, file, contentType);
    }

    /**
     * 上传对象
     *
     * @param objectName 对象名称
     * @param file       文件
     * @return {@link String} 文件名
     * @throws MinioException minio异常
     */
    public String putObject(String objectName, File file) throws MinioException {
        return this.putObject(minioProperties.getBucketName(), objectName, file);
    }

    /**
     * 上传对象
     *
     * @param file 文件
     * @return {@link String} 文件名
     * @throws MinioException minio异常
     */
    public String putObject(File file) throws MinioException {
        Assert.isTrue(FileUtil.exist(file), ExceptionEnum.FILE_NAME_CANNOT_BE_EMPTY.getValue());
        String extName = FileUtil.extName(file.getName());
        Assert.notBlank(extName, "非法文件名称：" + file.getName());
        return this.putObject(IdUtil.fastSimpleUUID() + DateUtil.current() + StrUtil.DOT + extName, file);
    }

    /**
     * 获取bucket策略
     *
     * @param bucketName bucket名称
     * @return {@link String} 策略
     * @throws MinioException minio异常
     */
    public String getBucketPolicy(String bucketName) throws MinioException {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        GetBucketPolicyArgs bucketPolicyArgs = GetBucketPolicyArgs.builder().bucket(bucketName).build();

        try {
            return minioClient.getBucketPolicy(bucketPolicyArgs);
        } catch (Exception e) {
            logger.error("获取bucket策略异常 {}", e.getMessage());
            throw new MinioException("获取bucket策略异常", e);
        }
    }

    /**
     * 制定bucket策略
     *
     * @param bucketName        bucket名称
     * @param bucketPolicyParam bucket策略参数
     * @return {@link Boolean}
     */
    public Boolean setBucketPolicy(String bucketName, BucketPolicyParam bucketPolicyParam) {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        SetBucketPolicyArgs bucketPolicyArgs = SetBucketPolicyArgs.builder().bucket(bucketName)
                .config(JSON.toJSONString(bucketPolicyParam)).build();

        try {
            minioClient.setBucketPolicy(bucketPolicyArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("设置bucket策略异常 {}", e.getMessage());
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
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        DeleteBucketPolicyArgs bucketPolicyArgs = DeleteBucketPolicyArgs.builder().bucket(bucketName).build();
        try {
            minioClient.deleteBucketPolicy(bucketPolicyArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("删除bucket 策略异常 {}", e.getMessage());
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
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        LifecycleConfiguration config = new LifecycleConfiguration(rules);

        try {
            minioClient.setBucketLifecycle(SetBucketLifecycleArgs.builder().bucket(bucketName).config(config).build());
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("设置 bucket生命周期异常 {}", e.getMessage());
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
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        try {
            minioClient.deleteBucketLifecycle(DeleteBucketLifecycleArgs.builder().bucket(bucketName).build());
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("删除bucket生命周期异常 {}", e.getMessage());
        }
        return Boolean.FALSE;
    }

    /**
     * 获取bucket生命周期
     *
     * @param bucketName bucket名称
     * @return {@link LifecycleConfiguration}
     * @throws MinioException minio异常
     */
    public LifecycleConfiguration getBucketLifecycle(String bucketName) throws MinioException {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        try {
            return minioClient.getBucketLifecycle(GetBucketLifecycleArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            logger.error("获取bucket生命周期异常 {}", e.getMessage());
            throw new MinioException("获取bucket生命周期异常", e);
        }
    }

    /**
     * 获取bucket通知
     *
     * @param bucketName bucket名称
     * @return {@link NotificationConfiguration}
     * @throws MinioException minio异常
     */
    public NotificationConfiguration getBucketNotification(String bucketName) throws MinioException {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        GetBucketNotificationArgs notificationArgs = GetBucketNotificationArgs.builder()
                .bucket(bucketName)
                .build();

        try {
            return minioClient.getBucketNotification(notificationArgs);
        } catch (Exception e) {
            logger.error("获取bucket通知异常 {}", e.getMessage());
            throw new MinioException("获取bucket通知异常", e);
        }
    }

    /**
     * 设置bucket通知
     *
     * @param bucketName          bucket名称
     * @param queueConfigurations 队列配置
     * @return {@link Boolean}
     */
    public Boolean setBucketNotification(String bucketName, List<QueueConfiguration> queueConfigurations) {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        NotificationConfiguration config = new NotificationConfiguration();
        config.setQueueConfigurationList(queueConfigurations);

        SetBucketNotificationArgs bucketNotificationArgs = SetBucketNotificationArgs.builder().bucket(bucketName)
                .config(config).build();

        try {
            minioClient.setBucketNotification(bucketNotificationArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("设置bucket 通知异常 {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除bucket通知
     *
     * @param bucketName bucket名称
     * @return {@link Boolean}
     */
    public Boolean deleteBucketNotification(String bucketName) {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        DeleteBucketNotificationArgs bucketNotificationArgs = DeleteBucketNotificationArgs.builder().bucket(bucketName).build();
        try {

            minioClient.deleteBucketNotification(bucketNotificationArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("删除bucket通知异常 {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 获取bucket复制配置
     *
     * @param bucketName bucket名称
     * @return {@link ReplicationConfiguration}
     * @throws MinioException minio异常
     */
    public ReplicationConfiguration getBucketReplication(String bucketName) throws MinioException {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        GetBucketReplicationArgs bucketReplicationArgs = GetBucketReplicationArgs.builder().bucket(bucketName).build();

        try {

            return minioClient.getBucketReplication(bucketReplicationArgs);
        } catch (Exception e) {
            logger.error("删除bucket复制配置异常 {}", e.getMessage());
            throw new MinioException("删除bucket复制配置异常", e);
        }

    }

    /**
     * 设置bucket复制
     *
     * @param bucketName bucket名称
     * @param rules      规则
     * @return {@link Boolean}
     */
    public Boolean setBucketReplication(String bucketName, List<ReplicationRule> rules) {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        ReplicationConfiguration config = new ReplicationConfiguration("REPLACE-WITH-ACTUAL-ROLE", rules);
        SetBucketReplicationArgs bucketReplicationArgs = SetBucketReplicationArgs.builder().bucket(bucketName).config(config).build();

        try {
            minioClient.setBucketReplication(bucketReplicationArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("设置bucket复制异常 {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除bucket复制
     *
     * @param bucketName bucket名称
     * @return {@link Boolean}
     */
    public Boolean deleteBucketReplication(String bucketName) {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        try {
            minioClient.deleteBucketReplication(DeleteBucketReplicationArgs.builder().bucket(bucketName).build());
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("删除bucket复制异常 {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 监听bucket的对象通知
     *
     * @param bucketName bucket名称
     * @param prefix     前缀
     * @param suffix     后缀
     * @param events     事件 , 支持的事件类型：https://docs.min.io/docs/minio-bucket-notification-guide.html
     * @return {@link List<NotificationRecords>} 事件集合
     * @throws MinioException minio异常
     */
    public List<NotificationRecords> listenBucketNotification(String bucketName, String prefix, String suffix, String[] events) throws MinioException {

        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        ListenBucketNotificationArgs notificationArgs = ListenBucketNotificationArgs.builder()
                .bucket(bucketName).prefix(prefix).suffix(suffix).events(events).build();

        try {

            List<NotificationRecords> eventList = CollectionUtil.newArrayList();

            CloseableIterator<Result<NotificationRecords>> bucketNotification = minioClient.listenBucketNotification(notificationArgs);
            while (bucketNotification.hasNext()) {
                eventList.add(bucketNotification.next().get());
            }
            return eventList;
        } catch (Exception e) {
            logger.error("监听bucket的对象通知异常 {}", e.getMessage());
            throw new MinioException(e.getMessage());
        }
    }

    /**
     * 监听bucket的对象通知
     *
     * @param bucketName bucket名称
     * @param events     事件 , 支持的事件类型：https://docs.min.io/docs/minio-bucket-notification-guide.html
     * @return {@link List<NotificationRecords>} 事件集合
     * @throws MinioException minio异常
     */
    public List<NotificationRecords> listenBucketNotification(String bucketName, String[] events) throws MinioException {
        return this.listenBucketNotification(bucketName, StrUtil.EMPTY, StrUtil.EMPTY, events);
    }

    /**
     * bucket 设置加密
     *
     * @param bucketName   bucket名称
     * @param sseAlgorithm 加密算法枚举
     * @return {@link Boolean}
     */
    public Boolean setBucketEncryption(String bucketName, SseAlgorithm sseAlgorithm) {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        SseConfiguration config = StrUtil.equals(SseAlgorithm.AES256.toString(), sseAlgorithm.toString())
                ?  SseConfiguration.newConfigWithSseS3Rule() : SseConfiguration.newConfigWithSseKmsRule(IdUtil.fastSimpleUUID());

        SetBucketEncryptionArgs bucketEncryptionArgs = SetBucketEncryptionArgs.builder().bucket(bucketName).config(config).build();

        try {
            minioClient.setBucketEncryption(bucketEncryptionArgs);
            return Boolean.TRUE;
        } catch (Exception e) {
            logger.error("bucket 设置加密异常 {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 获取bucket加密
     *
     * @param bucketName bucket名称
     * @return {@link SseConfigurationRule} bucket加密方式
     * @throws MinioException minio异常
     */
    public SseConfigurationRule getBucketEncryption(String bucketName) throws MinioException {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        GetBucketEncryptionArgs bucketEncryptionArgs = GetBucketEncryptionArgs.builder().bucket(bucketName).build();

        try {
            SseConfiguration configuration = minioClient.getBucketEncryption(bucketEncryptionArgs);
            return configuration.rule();
        }catch (Exception e) {
            logger.error("获取bucket加密异常 {}", e.getMessage());
            throw new MinioException("获取bucket加密异常", e);
        }
    }

    /**
     * 删除bucket加密
     *
     * @param bucketName bucket名称
     * @return {@link Boolean}
     */
    public Boolean deleteBucketEncryption(String bucketName) {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        DeleteBucketEncryptionArgs bucketEncryptionArgs = DeleteBucketEncryptionArgs.builder().bucket(bucketName).build();
        try {

            minioClient.deleteBucketEncryption(bucketEncryptionArgs);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("删除bucket加密异常 {}", e.getMessage());
        }

        return Boolean.FALSE;

    }

    /**
     * 获取bucket标签
     *
     * @param bucketName bucket名称
     * @return {@link Map<String, String>}  标签信息
     * @throws MinioException minio异常
     */
    public Map<String, String> getBucketTags(String bucketName) throws MinioException{
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        GetBucketTagsArgs bucketTagsArgs = GetBucketTagsArgs.builder().bucket(bucketName).build();
        try {
            Tags bucketTags = minioClient.getBucketTags(bucketTagsArgs);
            return bucketTags.get();
        }catch (Exception e) {
            logger.error("获取bucket标签异常 {}", e.getMessage());
            throw new MinioException("获取bucket标签异常", e);
        }

    }

    /**
     * 设置bucket标签
     *
     * @param bucketName bucket名称
     * @param tags       标签
     * @return {@link Boolean} 是否成功
     */
    public Boolean setBucketTags(String bucketName, Map<String, String> tags) {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        SetBucketTagsArgs bucketTagsArgs = SetBucketTagsArgs.builder().bucket(bucketName).tags(tags).build();
        try {
            minioClient.setBucketTags(bucketTagsArgs);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("设置bucket标签异常 {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 删除bucket标签
     *
     * @param bucketName bucket名称
     * @return {@link Boolean}
     */
    public Boolean deleteBucketTags(String bucketName) {
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        DeleteBucketTagsArgs bucketTagsArgs = DeleteBucketTagsArgs.builder().bucket(bucketName).build();
        try {
            minioClient.deleteBucketTags(bucketTagsArgs);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("删除bucket标签异常 {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 获取对象标签
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link Map}  标签信息
     * @throws MinioException minio异常
     */
    public Map<String, String> getObjectTags(String bucketName, String objectName) throws MinioException {
        Assert.notBlank(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY.getValue());
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        GetObjectTagsArgs tagsArgs = GetObjectTagsArgs.builder().bucket(bucketName).object(objectName).build();

        try {
            return minioClient.getObjectTags(tagsArgs).get();
        }catch (Exception e) {
            logger.error("获取对象标签异常 {}", e.getMessage());
            throw new MinioException("获取对象标签异常", e);
        }

    }

    /**
     * 获取对象标签
     *
     * @param objectName 对象名称
     * @return {@link Map}  标签信息
     * @throws MinioException minio异常
     */
    public Map<String, String> getObjectTags(String objectName) throws MinioException {
        return this.getObjectTags(minioProperties.getBucketName(), objectName);
    }

    /**
     * 设置对象标签
     *
     * @param bucketName bucket名称
     * @param tags       标签信息
     * @param objectName 对象名称
     * @return {@link Boolean}  是否成功
     */
    public Boolean setObjectTags(String bucketName, String objectName, Map<String, String> tags) {
        Assert.notBlank(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY.getValue());
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());
        SetObjectTagsArgs objectTagsArgs = SetObjectTagsArgs.builder().bucket(bucketName).object(objectName).tags(tags).build();

        try {
            minioClient.setObjectTags(objectTagsArgs);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("设置对象标签异常 {}", e.getMessage());
        }

        return Boolean.FALSE;
    }

    /**
     * 设置对象标签
     *
     * @param tags       标签信息
     * @param objectName 对象名称
     * @return {@link Boolean}  是否成功
     */
    public Boolean setObjectTags(String objectName, Map<String, String> tags) {
     return setObjectTags(minioProperties.getBucketName(), objectName, tags);
    }

    /**
     * 删除对象标签
     *
     * @param bucketName bucket名称
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean deleteObjectTags(String bucketName, String objectName) {

        Assert.notBlank(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY.getValue());
        Assert.notBlank(bucketName, ExceptionEnum.BUCKET_NAME_CANNOT_BE_EMPTY.getValue());

        DeleteObjectTagsArgs objectTagsArgs = DeleteObjectTagsArgs.builder().bucket(bucketName).object(objectName).build();

        try {
            minioClient.deleteObjectTags(objectTagsArgs);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("删除对象标签异常 {}", e.getMessage());
        }

        return Boolean.FALSE;

    }

    /**
     * 删除对象标签
     *
     * @param objectName 对象名称
     * @return {@link Boolean} 是否成功
     */
    public Boolean deleteObjectTags( String objectName) {
        Assert.notBlank(objectName, ExceptionEnum.OBJECT_NAME_CANNOT_BE_EMPTY.getValue());
        DeleteObjectTagsArgs objectTagsArgs = DeleteObjectTagsArgs.builder().bucket(minioProperties.getBucketName()).object(objectName).build();

        try {
            minioClient.deleteObjectTags(objectTagsArgs);
            return Boolean.TRUE;
        }catch (Exception e) {
            logger.error("删除对象标签异常 {}", e.getMessage());
        }

        return Boolean.FALSE;

    }

























































}