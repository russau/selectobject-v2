package com.mycompany.app;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.JSONInput;
import software.amazon.awssdk.services.s3.model.JSONOutput;
import software.amazon.awssdk.services.s3.model.OutputSerialization;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;
import software.amazon.awssdk.services.s3.model.selectobjectcontenteventstream.DefaultRecords;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;

/**
 * Sample Application.
 */
public class App {

  private static final SsmClient ssmClient = SsmClient.builder().build();
  private static final S3AsyncClient s3 = S3AsyncClient.builder().build();

  public static void main(String[] args) {
    readDragonData();
  }

  private static void readDragonData() {
    String bucketName = getBucketName();
    String key = getKey();
    String query = getQuery();
    JsonArray results = new JsonArray();

    TestHandler testHandler = new TestHandler();

    // See reference on AWS SDK for Java 2.x async programming
    // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/asynchronous.html
    CompletableFuture<Void> selected = queryS3(s3, bucketName, key, query, testHandler);
    selected.join();

    for (SelectObjectContentEventStream events : testHandler.receivedEvents) {
      if (events instanceof DefaultRecords) {
        DefaultRecords defaultRecords = (DefaultRecords) events;
        String payload = defaultRecords.payload().asString(StandardCharsets.UTF_8);

        JsonElement e = JsonParser.parseString(payload);
        JsonArray first = (JsonArray) e.getAsJsonObject().get("_1");
        results.addAll(first);
      }
    }

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    System.out.println(gson.toJson(results.toString()));
  }

  private static String getBucketName() {
    GetParameterRequest getParameterRequest = GetParameterRequest
        .builder()
        .name("dragon_data_bucket_name")
        .withDecryption(false).build();
    GetParameterResponse getParameterResponse = ssmClient.getParameter(getParameterRequest);
    return getParameterResponse.parameter().value();
  }

  private static String getKey() {
    GetParameterRequest getParameterRequest = GetParameterRequest
        .builder()
        .name("dragon_data_file_name")
        .withDecryption(false).build();
    GetParameterResponse getParameterResponse = ssmClient.getParameter(getParameterRequest);
    return getParameterResponse.parameter().value();
  }

  private static String getQuery() {
    // later on this method will return different results based
    // on query string parameters. For now, we will hardcode the results
    // to select *, which isn't the best showcase of S3 select
    // but don't worry we will get there
    return "select * from s3object s";
  }

  private static CompletableFuture<Void> queryS3(S3AsyncClient s3,
      String bucketName,
      String key,
      String query,
      SelectObjectContentResponseHandler handler) {
    InputSerialization inputSerialization = InputSerialization.builder()
        .json(JSONInput.builder().type("Document").build())
        .compressionType(CompressionType.NONE)
        .build();

    OutputSerialization outputSerialization = OutputSerialization.builder()
        .json(JSONOutput.builder().build())
        .build();
    
    SelectObjectContentRequest select = SelectObjectContentRequest.builder()
        .bucket(bucketName)
        .key(key)
        .expression(query)
        .expressionType(ExpressionType.SQL)
        .inputSerialization(inputSerialization)
        .outputSerialization(outputSerialization)
        .build();

    return s3.selectObjectContent(select, handler);
  }


  private static class TestHandler implements SelectObjectContentResponseHandler {
    private List<SelectObjectContentEventStream> receivedEvents = new ArrayList<>();

    @Override
    public void responseReceived(SelectObjectContentResponse response) { }

    @Override
    public void onEventStream(SdkPublisher<SelectObjectContentEventStream> publisher) {
      publisher.subscribe(receivedEvents::add);
    }

    @Override
    public void exceptionOccurred(Throwable throwable) { }

    @Override
    public void complete() { }
  }
}
