package cn.darkjrong.minio.config;

import cn.darkjrong.spring.boot.autoconfigure.MinioAutoConfiguration;
import cn.darkjrong.spring.boot.autoconfigure.MinioProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.minio.MinioClient;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.health.HealthContributorAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.web.server.ManagementContextAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

/**
 * minio指标配置
 *
 * @author Rong.Jia
 * @date 2021/08/08 19:35:13
 */
@Aspect
@Configuration
@EnableConfigurationProperties({MinioProperties.class})
@ConditionalOnClass({MinioClient.class, ManagementContextAutoConfiguration.class})
@ConditionalOnEnabledHealthIndicator("minio")
@AutoConfigureBefore(HealthContributorAutoConfiguration.class)
@AutoConfigureAfter(MinioAutoConfiguration.class)
public class MinioMetricConfiguration {

    private final MeterRegistry meterRegistry;
    private final MinioProperties minioProperties;

    private Timer listOkTimer;
    private Timer listKoTimer;
    private Timer getOkTimer;
    private Timer getKoTimer;
    private Timer putOkTimer;
    private Timer putKoTimer;
    private Timer removeOkTimer;
    private Timer removeKoTimer;
    private Timer listBucketOkTimer;
    private Timer listBucketKoTimer;

    public MinioMetricConfiguration(MeterRegistry meterRegistry, MinioProperties minioProperties) {
        this.meterRegistry = meterRegistry;
        this.minioProperties = minioProperties;
    }

    @PostConstruct
    public void initTimers() {
        listOkTimer = Timer
                .builder(minioProperties.getMetricName())
                .tag("operation", "listObjects")
                .tag("status", "ok")
                .tag("bucket", minioProperties.getBucketName())
                .register(meterRegistry);

        listKoTimer = Timer
                .builder(minioProperties.getMetricName())
                .tag("operation", "listObjects")
                .tag("status", "ko")
                .tag("bucket", minioProperties.getBucketName())
                .register(meterRegistry);

        getOkTimer = Timer
                .builder(minioProperties.getMetricName())
                .tag("operation", "getObject")
                .tag("status", "ok")
                .tag("bucket", minioProperties.getBucketName())
                .register(meterRegistry);

        getKoTimer = Timer
                .builder(minioProperties.getMetricName())
                .tag("operation", "getObject")
                .tag("status", "ko")
                .tag("bucket", minioProperties.getBucketName())
                .register(meterRegistry);

        putOkTimer = Timer
                .builder(minioProperties.getMetricName())
                .tag("operation", "putObject")
                .tag("status", "ok")
                .tag("bucket", minioProperties.getBucketName())
                .register(meterRegistry);

        putKoTimer = Timer
                .builder(minioProperties.getMetricName())
                .tag("operation", "putObject")
                .tag("status", "ko")
                .tag("bucket", minioProperties.getBucketName())
                .register(meterRegistry);

        listBucketOkTimer = Timer
                .builder(minioProperties.getMetricName() + ".list.bucket")
                .tag("operation", "listBuckets")
                .tag("status", "ok")
                .register(meterRegistry);

        listBucketKoTimer = Timer
                .builder(minioProperties.getMetricName() + ".list.bucket")
                .tag("operation", "listBuckets")
                .tag("status", "ko")
                .register(meterRegistry);

        removeOkTimer = Timer
                .builder(minioProperties.getMetricName())
                .tag("operation", "removeObject")
                .tag("status", "ok")
                .tag("bucket", minioProperties.getBucketName())
                .register(meterRegistry);

        removeKoTimer = Timer
                .builder(minioProperties.getMetricName())
                .tag("operation", "removeObject")
                .tag("status", "ko")
                .tag("bucket", minioProperties.getBucketName())
                .register(meterRegistry);
    }


    @ConditionalOnBean(MinioClient.class)
    @Around("execution(* io.minio.MinioClient.getObject(..))")
    public Object getMeter(ProceedingJoinPoint pjp) throws Throwable {
        long l = System.currentTimeMillis();

        try {
            Object proceed = pjp.proceed();
            getOkTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            return proceed;
        } catch (Exception e) {
            getKoTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            throw e;
        }
    }

    @ConditionalOnBean(MinioClient.class)
    @Around("execution(* io.minio.MinioClient.listObjects(..))")
    public Object listMeter(ProceedingJoinPoint pjp) throws Throwable {
        long l = System.currentTimeMillis();

        try {
            Object proceed = pjp.proceed();
            listOkTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            return proceed;
        } catch (Exception e) {
            listKoTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            throw e;
        }
    }

    @ConditionalOnBean(MinioClient.class)
    @Around("execution(* io.minio.MinioClient.putObject(..))")
    public Object putMeter(ProceedingJoinPoint pjp) throws Throwable {
        long l = System.currentTimeMillis();

        try {
            Object proceed = pjp.proceed();
            putOkTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            return proceed;
        } catch (Exception e) {
            putKoTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            throw e;
        }
    }

    @ConditionalOnBean(MinioClient.class)
    @Around("execution(* io.minio.MinioClient.listBuckets(..))")
    public Object listBucketMeter(ProceedingJoinPoint pjp) throws Throwable {
        long l = System.currentTimeMillis();

        try {
            Object proceed = pjp.proceed();
            listBucketOkTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            return proceed;
        } catch (Exception e) {
            listBucketKoTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            throw e;
        }
    }

    @ConditionalOnBean(MinioClient.class)
    @Around("execution(* io.minio.MinioClient.removeObject(..))")
    public Object removeMeter(ProceedingJoinPoint pjp) throws Throwable {
        long l = System.currentTimeMillis();

        try {
            Object proceed = pjp.proceed();
            removeOkTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            return proceed;
        } catch (Exception e) {
            removeKoTimer.record(System.currentTimeMillis() - l, TimeUnit.MILLISECONDS);
            throw e;
        }
    }
}
