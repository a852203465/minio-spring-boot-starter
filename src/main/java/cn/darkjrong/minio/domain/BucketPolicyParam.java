package cn.darkjrong.minio.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 桶政策参数
 *
 * https://docs.min.io/docs/minio-bucket-replication-guide.html
 *
 * @author Rong.Jia
 * @date 2021/08/05 11:03:11
 */
@NoArgsConstructor
@Data
public class BucketPolicyParam implements Serializable {

    private static final long serialVersionUID = 1866600240824156789L;


    /**
     * Statement : [{"Action":["s3:GetBucketLocation","s3:ListBucket"],"Effect":"Allow","Principal":"*","Resource":"arn:aws:s3:::my-bucketname"},{"Action":"s3:GetObject","Effect":"Allow","Principal":"*","Resource":"arn:aws:s3:::my-bucketname/myobject*"}]
     * Version : 2012-10-17
     */

    private String Version;
    private List<StatementBean> Statement;

    @NoArgsConstructor
    @Data
    public static class StatementBean {
        /**
         * Action : ["s3:GetBucketLocation","s3:ListBucket"]
         * Effect : Allow
         * Principal : *
         * Resource : arn:aws:s3:::my-bucketname
         */

        private String Effect;
        private String Principal;
        private String Resource;
        private List<String> Action;
    }
}
