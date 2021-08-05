package cn.darkjrong.minio.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * 删除对象
 *
 * @author Rong.Jia
 * @date 2021/08/04 22:51:18
 */
@Data
public class RemoveObject implements Serializable {

    private static final long serialVersionUID = 1646739658200067177L;

    /**
     *  bucket 名
     */
    private String bucketName;

    /**
     * 对象名
     */
    private String objectName;

    /**
     * 版本
     */
    private String versionId;


    public RemoveObject(String bucketName, String objectName, String versionId) {
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.versionId = versionId;
    }

    public RemoveObject(String bucketName, String objectName) {
        this.bucketName = bucketName;
        this.objectName = objectName;
    }

    public RemoveObject(String objectName) {
        this.objectName = objectName;
    }







}
