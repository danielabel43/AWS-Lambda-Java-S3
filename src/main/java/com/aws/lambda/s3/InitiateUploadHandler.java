package com.aws.lambda.s3;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class InitiateUploadHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final Regions clientRegion = Regions.AP_SOUTH_1;
    private static final String bucketName = "file-upload-t";

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
        S3Presigner presigner = S3Presigner.create();
        LambdaLogger logger = context.getLogger();
        logger.log("Loading Upload handler");
        logger.log("length "+String.valueOf(event.getBody().getBytes().length));
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        try {
            JSONObject uploadReqInJson=(JSONObject) new JSONParser().parse(event.getBody());
            logger.log("Remade JSON request"+uploadReqInJson.toString());

            //Extract info from the request and create meta-data
            Map<String,String> metaData=new HashMap<>();
            metaData.put("category", (String) uploadReqInJson.get("category"));
            metaData.put("image-type", (String) uploadReqInJson.get("image-type"));

            // Generate the pre-signed URL.
            logger.log("Generating pre-signed URL.");
            String objectKey=uploadReqInJson.get("filename")+"::"+uploadReqInJson.get("image-type");
            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .contentType((String) uploadReqInJson.get("image-type"))
                    .metadata(metaData)
                    .build();

            PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofMinutes(10))
                    .putObjectRequest(objectRequest)
                    .build();

            PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);
            String url = presignedRequest.url().toString();
            logger.log("Pre-Signed URL: " + url.toString());
            response.setBody(url.toString());
        }
        catch (Exception ex){
            logger.log(ex.getMessage());
            response.setBody("Error in lambda");
        }

        return response;
    }
}
