package cn.darkjrong.minio;

import cn.darkjrong.minio.enums.ExceptionEnum;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import io.minio.Result;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * minio工具类
 *
 * @author Rong.Jia
 * @date 2021/08/05 08:27:54
 */
class MinioUtils {

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

    /**
     * 获取日期文件夹
     *
     * @return {@link String} 日期文件夹
     */
    public static String getDateFolder() {
        return DateUtil.format(new Date(), DatePattern.PURE_DATE_FORMAT);
    }

    /**
     * 获取文件的名字
     *
     * @param fileName 文件名称
     * @return {@link String}
     */
    public static String getFileName(String fileName) {
        String extName = FileUtil.extName(fileName);
        Assert.notBlank(extName, "非法文件名称：" + fileName);
        String suffix = StrUtil.DOT + extName;
        return getDateFolder() + StrUtil.SLASH + IdUtil.fastUUID() + DateUtil.current() + suffix;
    }










}
