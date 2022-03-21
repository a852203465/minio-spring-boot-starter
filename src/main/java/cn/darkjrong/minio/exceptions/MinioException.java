package cn.darkjrong.minio.exceptions;

import cn.hutool.core.exceptions.ExceptionUtil;
import cn.hutool.core.util.StrUtil;
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

    public MinioException(Throwable e) {
        super(ExceptionUtil.getMessage(e), e);
    }

    public MinioException(String message) {
        super(message);
    }

    public MinioException(String messageTemplate, Object... params) {
        super(StrUtil.format(messageTemplate, params));
    }

    public MinioException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public MinioException(Throwable throwable, String messageTemplate, Object... params) {
        super(StrUtil.format(messageTemplate, params), throwable);
    }


}
