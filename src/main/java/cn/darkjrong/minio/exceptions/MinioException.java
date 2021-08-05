package cn.darkjrong.minio.exceptions;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * minio 异常
 *
 * @author Rong.Jia
 * @date 2021/08/04 19:44:55
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MinioException extends RuntimeException implements Serializable {

    private static final long serialVersionUID = -1064571676986804614L;

    public MinioException(String message) {
        super(message);
    }


}
