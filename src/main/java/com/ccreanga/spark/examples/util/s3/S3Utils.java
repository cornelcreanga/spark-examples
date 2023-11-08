package com.ccreanga.spark.examples.util.s3;

import scala.Tuple2;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class S3Utils {

    S3Client s3Client;

    public S3Utils(String user, String password, String endpoint) throws URISyntaxException {
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(user, password));
        s3Client = S3Client.builder()
                .forcePathStyle(true)
                .endpointOverride(new URI(endpoint))
                .credentialsProvider(credentialsProvider)
                .build();

    }

    public void createBucket(String bucketName){
        S3Waiter s3Waiter = s3Client.waiter();
        CreateBucketRequest request = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();

        s3Client.createBucket(request);
        HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();

        WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
        waiterResponse.matched().response().ifPresent(response -> System.out.println(response));
    }

    public List<Tuple2<String,Long>> keys(String bucketName, String prefix) {

        String nextContinuationToken = null;
        List<Tuple2<String,Long>> result = new ArrayList<>();

        do {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .continuationToken(nextContinuationToken);

            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
            nextContinuationToken = response.nextContinuationToken();

            result.addAll( response.contents().stream().map(s3Object -> new Tuple2<>(s3Object.key(), s3Object.size())).toList());

        } while (nextContinuationToken != null);
        return result;

    }

    public void dropBucket(String bucketName){
        DeleteBucketRequest  request = DeleteBucketRequest.builder().bucket(bucketName).build();
        s3Client.deleteBucket(request);
    }


}
