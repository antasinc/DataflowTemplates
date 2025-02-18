/*
 * Copyright (C) 2018 Google Inc.
 * Copyright (C) 2019 Antas Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.templates.TextToBigQueryStreaming.wrapBigQueryInsertError;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.util.ResourceUtils;
import com.google.cloud.teleport.util.ValueProviderUtils;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubToBigQueryMultipleTables} pipeline is a streaming pipeline
 * which ingests data in JSON format from Cloud Pub/Sub, executes a UDF, and
 * outputs the resulting records to BigQuery. Any errors which occur in the
 * transformation of the data or execution of the UDF will be output to a
 * separate errors table in BigQuery. The errors table will be created if it
 * does not exist prior to execution. Both output and error tables are specified
 * by the user as template parameters.
 *
 * <p>
 * <b>Pipeline Requirements</b>
 *
 * <ul>
 * <li>The Pub/Sub topic exists.
 * <li>The BigQuery output table exists.
 * </ul>
 *
 * <p>
 * <b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-bigquery
 * USE_SUBSCRIPTION=true or false depending on whether the pipeline should read
 *                  from a Pub/Sub Subscription or a Pub/Sub Topic.
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToBigQueryMultipleTables \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}
 * --useSubscription=${USE_SUBSCRIPTION}
 * "
 *
 * # Execute the template
 * JOB_NAME=pubsub-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * # Execute a pipeline to read from a Topic.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputTopic=projects/${PROJECT_ID}/topics/input-topic-name,\
 * outputDataset=${PROJECT_ID}:dataset-id,\
 * outputTables=table1|table2|table3,\
 * outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table"
 *
 * # Execute a pipeline to read from a Subscription.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputSubscription=projects/${PROJECT_ID}/subscriptions/input-subscription-name,\
 * outputDataset=${PROJECT_ID}:dataset-id,\
 * outputTables=table1|table2|table3,\
 * outputDeadletterTable=${PROJECT_ID}:dataset-id.deadletter-table"
 * </pre>
 */
public class PubSubToBigQueryMultipleTables {

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQueryMultipleTables.class);

  /** table_name field in Pub/Sub message. */
  private static final String TABLE_NAME_FIELD = "bq_table";

  /** The tag for the main output for the UDF. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_OUT = new TupleTag<FailsafeElement<PubsubMessage, String>>() {
  };

  /** The tag for the main output of the json transformation. */
  @SuppressWarnings("serial")
  public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<TableRow>() {
  };

  /** The tag for the dead-letter output of the udf. */
  @SuppressWarnings("serial")
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_DEADLETTER_OUT = new TupleTag<FailsafeElement<PubsubMessage, String>>() {
  };

  /** The tag for the dead-letter output of the json to table row transform. */
  @SuppressWarnings("serial")
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_DEADLETTER_OUT = new TupleTag<FailsafeElement<PubsubMessage, String>>() {
  };

  /**
   * The default suffix for error tables if dead letter table is not specified.
   * CAUTION: DO NOT FORGET TO PLACE "." in front of table name.
   */
  public static final String DEFAULT_DEADLETTER_TABLE_SUFFIX = ".error_records";

  /** Pubsub message/string coder for pipeline. */
  public static final FailsafeElementCoder<PubsubMessage, String> CODER = FailsafeElementCoder
      .of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

  /** String/String Coder for FailsafeElement. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER = FailsafeElementCoder
      .of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /**
   * The {@link Options} class provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface Options extends PipelineOptions, JavascriptTextTransformerOptions {
    @Description("Dataset which owns the tables to write the output to")
    @Validation.Required
    ValueProvider<String> getOutputDataset();

    void setOutputDataset(ValueProvider<String> value);

    @Description("Tables to write the output to in the dataset, `|`-separated")
    @Validation.Required
    ValueProvider<String> getOutputTables();

    void setOutputTables(ValueProvider<String> value);

    @Description("Pub/Sub topic to read the input from")
    ValueProvider<String> getInputTopic();

    void setInputTopic(ValueProvider<String> value);

    @Description("The Cloud Pub/Sub subscription to consume from. " + "The name should be in the format of "
        + "projects/<project-id>/subscriptions/<subscription-name>.")
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> value);

    @Description("This determines whether the template reads from " + "a pub/sub subscription or a topic")
    @Default.Boolean(false)
    Boolean getUseSubscription();

    void setUseSubscription(Boolean value);

    @Description("The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
        + "format. If it doesn't exist, it will be created during pipeline execution.")
    ValueProvider<String> getOutputDeadletterTable();

    void setOutputDeadletterTable(ValueProvider<String> value);
  }

  /**
   * The main entry-point for pipeline execution. This method will start the
   * pipeline but will not wait for it's execution to finish. If blocking
   * execution is required, use the
   * {@link PubSubToBigQueryMultipleTables#run(Options)} method to start the
   * pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does
   * not wait until the pipeline is finished before returning. Invoke
   * {@code result.waitUntilFinish()} on the result object to block until the
   * pipeline is finished running if blocking programmatic execution is required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

    /*
     * Steps: 1) Read messages in from Pub/Sub 2) Transform the PubsubMessages into
     * TableRows - Transform message payload via UDF - Convert UDF result to
     * TableRow objects 3) Write successful records out to BigQuery 4) Write failed
     * records out to BigQuery
     */

    /*
     * Step #1: Read messages in from Pub/Sub Either from a Subscription or Topic
     */
    PCollection<PubsubMessage> messages = null;
    if (options.getUseSubscription()) {
      messages = pipeline.apply("ReadPubSubSubscription",
          PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()));
    } else {
      messages = pipeline.apply("ReadPubSubTopic",
          PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
    }

    /*
     * Step #2: Transform the PubsubMessages into TableRows
     */
    PCollectionTuple convertedTableRows = messages.apply("ConvertMessageToTableRow", new PubsubMessageToTableRow(options));

    /*
     * Step #3: Write the successful records out to BigQuery
     */
    WriteResult writeResult = convertedTableRows.get(TRANSFORM_OUT).apply("WriteSuccessfulRecords",
        BigQueryIO.writeTableRows().withoutValidation().withCreateDisposition(CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withExtendedErrorInfo()
            .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
            // Change the target BQ table dynamically by the value associated to the field name (defined by TABLE_NAME_FIELD)
            // in a message from Pub/Sub
            .to(input -> new TableDestination((String) (input.getValue().get(TABLE_NAME_FIELD)), "Destination")));

    /*
     * Step 3 Contd. Elements that failed inserts into BigQuery are extracted and
     * converted to FailsafeElement
     */
    PCollection<FailsafeElement<String, String>> failedInserts = writeResult.getFailedInsertsWithErr()
        .apply("WrapInsertionErrors", MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
            .via((BigQueryInsertError e) -> wrapBigQueryInsertError(e)))
        .setCoder(FAILSAFE_ELEMENT_CODER);

    /*
     * Step #4: Write records that failed table row transformation or conversion out
     * to BigQuery deadletter table.
     */
    PCollectionList
        .of(ImmutableList.of(convertedTableRows.get(UDF_DEADLETTER_OUT),
            convertedTableRows.get(TRANSFORM_DEADLETTER_OUT)))
        .apply("Flatten", Flatten.pCollections()).apply("WriteFailedRecords",
            ErrorConverters.WritePubsubMessageErrors.newBuilder()
                .setErrorRecordsTable(ValueProviderUtils.maybeUseDefaultDeadletterTable(
                    options.getOutputDeadletterTable(),
                    options.getOutputDataset(), DEFAULT_DEADLETTER_TABLE_SUFFIX))
                .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson()).build());

    // 5) Insert records that failed insert into deadletter table
    failedInserts.apply("WriteFailedRecords",
        ErrorConverters.WriteStringMessageErrors.newBuilder()
            .setErrorRecordsTable(ValueProviderUtils.maybeUseDefaultDeadletterTable(
              options.getOutputDeadletterTable(),
              options.getOutputDataset(), DEFAULT_DEADLETTER_TABLE_SUFFIX))
            .setErrorRecordsTableSchema(ResourceUtils.getDeadletterTableSchemaJson()).build());

    return pipeline.run();
  }

  /**
   * The {@link PubsubMessageToTableRow} class is a {@link PTransform} which
   * transforms incoming {@link PubsubMessage} objects into {@link TableRow}
   * objects for insertion into BigQuery while applying an optional UDF to the
   * input. The executions of the UDF and transformation to {@link TableRow}
   * objects is done in a fail-safe way by wrapping the element with it's original
   * payload inside the {@link FailsafeElement} class. The
   * {@link PubsubMessageToTableRow} transform will output a
   * {@link PCollectionTuple} which contains all output and dead-letter
   * {@link PCollection}.
   *
   * <p>
   * The {@link PCollectionTuple} output will contain the following
   * {@link PCollection}:
   *
   * <ul>
   * <li>{@link PubSubToBigQueryMultipleTables#UDF_OUT} - Contains all
   * {@link FailsafeElement} records successfully processed by the optional UDF.
   * <li>{@link PubSubToBigQueryMultipleTables#UDF_DEADLETTER_OUT} - Contains all
   * {@link FailsafeElement} records which failed processing during the UDF
   * execution.
   * <li>{@link PubSubToBigQueryMultipleTables#TRANSFORM_OUT} - Contains all
   * records successfully converted from JSON to {@link TableRow} objects.
   * <li>{@link PubSubToBigQueryMultipleTables#TRANSFORM_DEADLETTER_OUT} -
   * Contains all {@link FailsafeElement} records which couldn't be converted to
   * table rows.
   * </ul>
   */
  static class PubsubMessageToTableRow extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

    private final Options options;

    PubsubMessageToTableRow(Options options) {
      this.options = options;
    }

    @Override
    public PCollectionTuple expand(PCollection<PubsubMessage> input) {
      PCollectionTuple udfOut = input
          // Map the incoming messages into FailsafeElements so we can recover from
          // failures
          // across multiple transforms.
          .apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn())).apply("InvokeUDF",
              FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                  .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                  .setFunctionName(options.getJavascriptTextTransformFunctionName()).setSuccessTag(UDF_OUT)
                  .setFailureTag(UDF_DEADLETTER_OUT).build());

      // Convert the records which were successfully processed by the UDF into
      // TableRow objects.
      PCollectionTuple jsonToTableRowOut = udfOut.get(UDF_OUT).apply("JsonToTableRow", FailsafeJsonToTableRow
          .<PubsubMessage>newBuilder().setSuccessTag(TRANSFORM_OUT).setFailureTag(TRANSFORM_DEADLETTER_OUT).build());

      // Re-wrap the PCollections so we can return a single PCollectionTuple
      return PCollectionTuple.of(UDF_OUT, udfOut.get(UDF_OUT)).and(UDF_DEADLETTER_OUT, udfOut.get(UDF_DEADLETTER_OUT))
          .and(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
          .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
    }
  }

  /**
   * The {@link PubsubMessageToFailsafeElementFn} wraps an incoming
   * {@link PubsubMessage} with the {@link FailsafeElement} class so errors can be
   * recovered from and the original message can be output to a error records
   * table.
   */
  static class PubsubMessageToFailsafeElementFn extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      context.output(FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
    }
  }

  /**
   * The {@link FailsafeJsonToTableRow} transform converts JSON strings to
   * {@link TableRow} objects. The transform accepts a {@link FailsafeElement}
   * object so the original payload of the incoming record can be maintained
   * across multiple series of transforms. **This is copied from
   * BigQueryConverters and modified to check if the value of table name attribute
   * exists.**
   */
  @AutoValue
  abstract static class FailsafeJsonToTableRow<T>
      extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

    public abstract TupleTag<TableRow> successTag();

    public abstract TupleTag<FailsafeElement<T, String>> failureTag();

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_PubSubToBigQueryMultipleTables_FailsafeJsonToTableRow.Builder<>();
    }

    /** Builder for {@link FailsafeJsonToTableRow}. */
    @AutoValue.Builder
    public abstract static class Builder<T> {
      public abstract Builder<T> setSuccessTag(TupleTag<TableRow> successTag);

      public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

      public abstract FailsafeJsonToTableRow<T> build();
    }

    /**
     * Modified for the BigQuery table name check from original.
     */
    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> failsafeElements) {
      return failsafeElements.apply("JsonToTableRow", ParDo.of(new DoFn<FailsafeElement<T, String>, TableRow>() {
        @ProcessElement
        public void processElement(ProcessContext context) {
          FailsafeElement<T, String> element = context.element();
          String json = element.getPayload();

          try {
            TableRow row = convertJsonToTableRow(json);
            String tableName = (String)row.get(TABLE_NAME_FIELD);
            if (StringUtils.isBlank(tableName)) {
              throw new RuntimeException("No BigQuery table name field: " + TABLE_NAME_FIELD);
            }
            if (!tableName.contains(".")) {
              tableName = context.getPipelineOptions().as(Options.class).getOutputDataset() + "." + tableName;
            }

            if (!isAllowedTable(context, tableName)) {
              throw new RuntimeException("Destination table is not allowed. check outputTables option: Is "
                + tableName
                + " in " + context.getPipelineOptions().as(Options.class).getOutputTables() + "?");
            }

            row.set(TABLE_NAME_FIELD, tableName);
            context.output(row);
          } catch (Exception e) {
            context.output(failureTag(), FailsafeElement.of(element).setErrorMessage(e.getMessage())
                .setStacktrace(Throwables.getStackTraceAsString(e)));
          }
        }

        private boolean isAllowedTable(ProcessContext context, String tableName) {
          String[] splitted = tableName.split(Pattern.quote("."));
          String plainTableName = splitted[splitted.length - 1];
          String[] allowedTables = context.getPipelineOptions().as(Options.class).getOutputTables().get().split(Pattern.quote("|"));

          return Arrays.asList(allowedTables).contains(plainTableName);
        }
      }).withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }

    /**
     * **This method is copied from BigQueryConverter** to be used in this class.**
     * 
     * Converts a JSON string to a {@link TableRow} object. If the data fails to
     * convert, a {@link RuntimeException} will be thrown.
     *
     * @param json The JSON string to parse.
     * @return The parsed {@link TableRow} object.
     */
    private static TableRow convertJsonToTableRow(String json) {
      TableRow row;
      // Parse the JSON into a {@link TableRow} object.
      try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
        // FIXME: There is warning around `Context.OUTER` but the solution has not been found
        row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
      } catch (IOException e) {
        throw new RuntimeException("Failed to serialize json to table row: " + json, e);
      }

      return row;
    }
  }
}
