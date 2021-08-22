package cn.darkjrong.minio;

import cn.darkjrong.minio.exceptions.ExceptionEnum;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ObjectUtil;
import io.minio.Result;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * minio工具类
 *
 * @author Rong.Jia
 * @date 2021/08/05 08:27:54
 */
public class MinioUtils {

    private static final Logger logger = LoggerFactory.getLogger(MinioUtils.class);

    /**
     * 数据转换
     *
     * @param objects 返回列表
     * @return 对象集合
     */
    protected static List<Item> getItems(Iterable<Result<Item>> objects) {
        return StreamSupport
                .stream(objects.spliterator(), true)
                .map(itemResult -> {
                    try {
                        return itemResult.get();
                    } catch (Exception e) {
                        logger.error("Error while parsing list of objects {}", e.getMessage());
                    }
                    return null;
                }).filter(ObjectUtil::isNotNull).collect(Collectors.toList());
    }

    /**
     * 判断是否为空
     *
     * @param object 对象
     * @param exceptionEnum 异常枚举
     * @throws IllegalArgumentException 非法参数异常
     */
    protected static void notEmpty(String object, ExceptionEnum exceptionEnum) throws IllegalArgumentException {
        Assert.notBlank(object, exceptionEnum.getValue());
    }

    /**
     * 判断是否为空
     *
     * @param objects 对象集合
     * @param exceptionEnum 异常枚举
     * @throws IllegalArgumentException 非法参数异常
     */
    protected static void notEmpty(Collection<?> objects, ExceptionEnum exceptionEnum) throws IllegalArgumentException {
        Assert.notEmpty(objects, exceptionEnum.getValue());
    }

    /**
     * 判断是否为空
     *
     * @param object 对象
     * @param exceptionEnum 异常枚举
     * @throws IllegalArgumentException 非法参数异常
     */
    protected static void notEmpty(Object object, ExceptionEnum exceptionEnum) throws IllegalArgumentException {
        Assert.notNull(object, exceptionEnum.getValue());
    }











}
