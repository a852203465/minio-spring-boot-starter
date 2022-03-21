package cn.darkjrong.minio.config;

import cn.darkjrong.minio.MinioTemplate;
import cn.darkjrong.minio.annotations.MinioNotification;
import cn.darkjrong.spring.boot.autoconfigure.MinioAutoConfiguration;
import cn.darkjrong.spring.boot.autoconfigure.MinioProperties;
import cn.hutool.core.collection.CollectionUtil;
import io.minio.messages.NotificationRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * minio通知配置
 *
 * @author Rong.Jia
 * @date 2021/08/08 19:35:37
 */
@Configuration
@EnableConfigurationProperties({MinioProperties.class})
@AutoConfigureBefore(MinioMetricConfiguration.class)
@AutoConfigureAfter(MinioAutoConfiguration.class)
public class MinioNotificationConfiguration implements ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(MinioNotificationConfiguration.class);

    private final MinioTemplate minioTemplate;
    private final MinioProperties minioProperties;

    private List<Thread> handlers = new ArrayList<>();

    public MinioNotificationConfiguration(MinioTemplate minioTemplate, MinioProperties minioProperties) {
        this.minioTemplate = minioTemplate;
        this.minioProperties = minioProperties;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Object obj = applicationContext.getBean(beanName);

            Class<?> objClz = obj.getClass();
            if (AopUtils.isAopProxy(obj)) {
                objClz = AopUtils.getTargetClass(obj);
            }

            for (Method m : objClz.getDeclaredMethods()) {
                if (m.isAnnotationPresent(MinioNotification.class)) {
                    //Check if has NotificationInfo parameter only
                    if (m.getParameterCount() != 1) {
                        throw new IllegalArgumentException("Minio notification handler should have only one NotificationInfo parameter");
                    }

                    if (m.getParameterTypes()[0] != NotificationRecords.class) {
                        throw new IllegalArgumentException("Parameter should be instance of NotificationRecords");
                    }

                    MinioNotification annotation = m.getAnnotation(MinioNotification.class);

                    //Then registering method handler
                    Thread handler = new Thread(() -> {
                        for (; ; ) {
                            try {
                                logger.info("Registering Minio handler on {} with notification {}", m.getName(), Arrays.toString(annotation.value()));
                                try {
                                    List<NotificationRecords> eventList = minioTemplate.listenBucketNotification(minioProperties.getBucketName(), annotation.prefix(),
                                            annotation.suffix(), annotation.value());
                                    if (CollectionUtil.isNotEmpty(eventList)) {
                                        for (Iterator<NotificationRecords> iterator = eventList.iterator(); iterator.hasNext();) {
                                            try {
                                                logger.debug("Receive notification for method {}", m.getName());
                                                m.invoke(obj, iterator.next());
                                            } catch (IllegalAccessException | InvocationTargetException e) {
                                                logger.error("Error while handling notification for method {} with notification {}", m.getName(), Arrays.toString(annotation.value()));
                                            }
                                        }
                                    }
                                }catch (Exception e) {
                                    logger.error("Getting a listener exception {} , method : {}", e.getMessage(), m.getName());
                                }
                            } catch (Exception e) {
                                logger.error("Error while registering notification for method " + m.getName() + " with notification " + Arrays.toString(annotation.value()), e);
                                throw new IllegalStateException("Cannot register handler", e);
                            }
                        }
                    });
                    handler.start();
                    handlers.add(handler);
                }
            }
        }
    }
}
