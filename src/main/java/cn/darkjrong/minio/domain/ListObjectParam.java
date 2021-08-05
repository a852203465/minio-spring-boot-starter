package cn.darkjrong.minio.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * 查询Object 参数
 *
 * @author Rong.Jia
 * @date 2021/08/04 23:48:58
 */
@Data
public class ListObjectParam implements Serializable {

    private static final long serialVersionUID = 2367399492414635479L;

    /**
     *  bucket 名
     */
    private String bucketName;

    private String startAfter;

    /**
     *  前缀
     */
    private String prefix;

    /**
     *  最大数量
     */
    private Integer maxKeys = 1000;

    /**
     *  是否包含版本
     */
    private boolean includeVersions;















}
