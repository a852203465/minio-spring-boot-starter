package cn.darkjrong.minio.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 异常枚举
 *
 * @author Rong.Jia
 * @date 2021/08/22
 */
@Getter
@AllArgsConstructor
public enum ExceptionEnum {

    // bucket 不能为空
    BUCKET_NAME_CANNOT_BE_EMPTY("Bucket cannot be empty"),

    OBJECT_NAME_CANNOT_BE_EMPTY("object cannot be empty"),
    FILE_NAME_CANNOT_BE_EMPTY("file name cannot be empty"),
    SOURCE_BUCKET_CANNOT_BE_EMPTY("source bucket cannot be empty"),
    TARGET_BUCKET_CANNOT_BE_EMPTY("target bucket cannot be empty"),
    SOURCE_OBJECT_CANNOT_BE_EMPTY("source object cannot be empty"),
    TARGET_OBJECT_CANNOT_BE_EMPTY("target object cannot be empty"),
    THE_OBJECT_COLLECTION_CANNOT_BE_EMPTY("The object collection cannot be empty"),
    VERSION_STATE_CANNOT_BE_EMPTY("bucket version state cannot be empty"),


    ;







    private String value;



}
