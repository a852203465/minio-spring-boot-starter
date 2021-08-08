package cn.darkjrong.minio.annotations;

import org.springframework.scheduling.annotation.Async;

import java.lang.annotation.*;

/**
 * minio通知注解
 *
 * @author Rong.Jia
 * @date 2021/08/08 19:10:10
 */
@Async
@Inherited
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MinioNotification {

    /**
     * 事件定义
     *  事件类型参考：https://docs.min.io/docs/minio-bucket-notification-guide.html
     *
     */
    String[] value();

    /**
     * 前缀
     */
    String prefix() default "";

    /**
     * 后缀
     */
    String suffix() default "";








}
