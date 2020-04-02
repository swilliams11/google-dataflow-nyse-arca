/*
 * Copyright (C) 2018 Google Inc.
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

/**
 * To run execute the following.
 export BUCKET=sw-hpc-dataflow-tutorial/dataflow-java-nyse-arca
 export project=sw-hpc
 mvn compile exec:java -Dexec.mainClass=com.google.cloud.teleport.templates.NyseArcaTransform -Dexec.cleanupDaemonThreads=false -Dexec.args=" \
 --project=$PROJECT \
 --stagingLocation=gs://$BUCKET/staging \
 --tempLocation=gs://$BUCKET/temp \
 --templateLocation=gs://$BUCKET/nyse-arca-book-job-spec.json \
 --runner=DataflowRunner"
 */

/**
 * Then execute.
 gcloud dataflow jobs run nyse-arca-book-java \
 --gcs-location=gs://$BUCKET/nyse-arca-book-job-spec.json \
 --zone=us-central1-f \
 --parameters=JSONPath=gs://$BUCKET/nyse-arca-book-job-spec.json,inputFilePattern=gs://sw-hpc-dataflow-tutorial/decompressed/EQY_US_ALL_ARCA_BOOK_20130404.csv,outputTable=$PROJECT:nyse_arca_java.eqy_arca_book_20130403,bigQueryLoadingTemporaryDirectory=gs://$BUCKET/bq_load_temp/
 --num-workers=5
*/

/**
 * This is the 10000 record test
 *
export BUCKET=sw-hpc-dataflow-tutorial/dataflow-java-nyse-arca
 export PROJECT=sw-hpc

 mvn compile exec:java -Dexec.mainClass=com.google.cloud.teleport.templates.NyseArcaTransform -Dexec.cleanupDaemonThreads=false -Dexec.args=" \
 --project=$PROJECT \
 --stagingLocation=gs://$BUCKET/staging \
 --tempLocation=gs://$BUCKET/temp \
 --templateLocation=gs://$BUCKET/nyse-arca-book-all-job-spec.json \
 --runner=DataflowRunner"


This is the updated code with 10000 records.
 gcloud dataflow jobs run nyse-arca-book-java \
 --gcs-location=gs://$BUCKET/nyse-arca-book-job-spec.json \
 --zone=us-central1-f \
 --parameters=JSONPath=gs://$BUCKET/big-query-schema.json,inputFilePattern=gs://sw-hpc-dataflow-tutorial/decompressed/EQY_US_ALL_ARCA_BOOK_20130403_smallfile.csv,outputTable=$PROJECT:nyse_arca_java.eqy_arca_book_20130403_all,bigQueryLoadingTemporaryDirectory=gs://$BUCKET/bq_load_temp/
 --num-workers=5
 */

package com.swilliams11.googlecloud.dataflow.nyse.arca;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.templates.common.BigQueryConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Pipeline to read text from TextIO, process the data, and write it to GCS.
 */
public class NyseArcaTransform4 {

  /** Options supported by {@link NyseArcaTransform4}. */
  public interface Options extends DataflowPipelineOptions, JavascriptTextTransformerOptions {

    @Description("The GCS location of the text you'd like to process")
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description("JSON file with BigQuery Schema description")
    ValueProvider<String> getJSONPath();

    void setJSONPath(ValueProvider<String> value);

    @Description("Output topic to write to")
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> value);


    @Validation.Required
    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);

    @Override
    DataflowProfilingAgentConfiguration getProfilingAgentConfiguration();
  }

  private static final Logger LOG = LoggerFactory.getLogger(NyseArcaTransform4.class);

  private static final String BIGQUERY_SCHEMA = "BigQuery Schema";
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String MODE = "mode";
  private static final int YEAR = 2013;
  private static final int MONTH = 4;
  private static final int DAY = 3;
  private static final Gson gson = new Gson();

  private static final TupleTag<String> addOrder =
          new TupleTag<String>(){};
  private static final TupleTag<String> deleteOrder =
          new TupleTag<String>(){};
  private static final TupleTag<String> modifyOrder =
          new TupleTag<String>(){};
  private static final TupleTag<String> imbalanceMessage =
          new TupleTag<String>(){};
  private static final TupleTag<String> systemEvent =
          new TupleTag<String>(){};

  private static String [] argsVar;
  private static Options optionsVar;

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    optionsVar = options;
    Pipeline pipeline = Pipeline.create(options);

    PCollectionTuple taggedRecords = pipeline
        .apply("Read from source", TextIO.read().from(options.getInputFilePattern()))
        .apply("Group NYSE Arca records", ParDo.of(new GroupNyseArcaRecord()).withOutputTags(addOrder,
            TupleTagList.of(deleteOrder)
                    .and(modifyOrder)
                    .and(imbalanceMessage)
                    .and(systemEvent)));
        //pipeline.run();

        taggedRecords.get(addOrder)
                .apply("Parse add records and convert CSV rows to JSON objects ", ParDo.of(new ParseAddRecords()))
                .apply(BigQueryConverters.jsonToTableRow())
                .apply("Insert into BigQuery", bigquery());

        taggedRecords.get(deleteOrder)
                .apply("Parse delete records and convert CSV rows to JSON objects ", ParDo.of(new ParseDeleteRecords()))
                .apply(BigQueryConverters.jsonToTableRow())
              .apply("Insert into BigQuery", bigquery());

        PCollection modifyOrdersCollection = taggedRecords.get(modifyOrder)
                .apply("Parse modify records and convert CSV rows to JSON objects ", ParDo.of(new ParseModifyRecords()))
                .apply(BigQueryConverters.jsonToTableRow());
        modifyOrdersCollection.apply("Insert into BigQuery", bigquery());

        PCollection imbalanceRecordsCollection = taggedRecords.get(imbalanceMessage)
                .apply("Parse imbalance records and convert CSV rows to JSON objects ", ParDo.of(new ParseImbalance()))
                .apply(BigQueryConverters.jsonToTableRow());
        imbalanceRecordsCollection.apply("Insert into BigQuery", bigquery());

        /*PCollection<String> systemEventsCollection = taggedRecords.get(systemEvent)
                .apply("Parse system events and convert CSV rows to JSON objects ", ParDo.of(new ParseAddRecords()));*/

    pipeline.run();
  }


  /**
   * This method separates the records into 5 groups - add, delete, modify and imbalance and system event.
   */
  static class GroupNyseArcaRecord extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String row, MultiOutputReceiver out) {
      // Use OutputReceiver.output to emit the output element.
      char messageType = row.charAt(0);
      switch(messageType){
        case 'A':
          out.get(addOrder).output(row);
          break;
        case 'M':
          out.get(modifyOrder).output(row);
          break;
        case 'D':
          out.get(deleteOrder).output(row);
          break;
        case 'I':
          out.get(imbalanceMessage).output(row);
          break;
        case 'V':
          out.get(systemEvent).output(row);
          break;
      }
    }
  }

  static  BigQueryIO.Write<TableRow> bigquery (){
      return BigQueryIO.writeTableRows()
              .withSchema(
                      NestedValueProvider.of(
                              optionsVar.getJSONPath(),
                              new SerializableFunction<String, TableSchema>() {

                                @Override
                                public TableSchema apply(String jsonPath) {

                                  TableSchema tableSchema = new TableSchema();
                                  List<TableFieldSchema> fields = new ArrayList<>();
                                  SchemaParser schemaParser = new SchemaParser();
                                  JSONObject jsonSchema;

                                  try {

                                    jsonSchema = schemaParser.parseSchema(jsonPath);

                                    JSONArray bqSchemaJsonArray =
                                            jsonSchema.getJSONArray(BIGQUERY_SCHEMA);

                                    for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
                                      JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
                                      TableFieldSchema field =
                                              new TableFieldSchema()
                                                      .setName(inputField.getString(NAME))
                                                      .setType(inputField.getString(TYPE));

                                      if (inputField.has(MODE)) {
                                        field.setMode(inputField.getString(MODE));
                                      }

                                      fields.add(field);
                                    }
                                    tableSchema.setFields(fields);

                                  } catch (Exception e) {
                                    throw new RuntimeException(e);
                                  }
                                  return tableSchema;
                                }
                              }))
              .to(optionsVar.getOutputTable())
              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              .withCustomGcsTempLocation(optionsVar.getBigQueryLoadingTemporaryDirectory());
  }

  /**
   * Process Add records.
   */
  static class ParseAddRecords extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<String> out) {
      String[] lineArray = row.split(",");
          AddOrderRecord a = new AddOrderRecord(lineArray);
          String json = a.toJSON();
          //LOG.info(json);
          out.output(json);
      }
    }

  /**
   * Process delete records.
   */
  static class ParseDeleteRecords extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<String> out) {
      String[] lineArray = row.split(",");
      DeleteRecord a = new DeleteRecord(lineArray);
      String json = a.toJSON();
      //LOG.info(json);
      out.output(json);
    }
  }

  /**
   * Process modify records.
   */
  static class ParseModifyRecords extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<String> out) {
      String[] lineArray = row.split(",");
      ModifyRecord a = new ModifyRecord(lineArray);
      String json = a.toJSON();
      //LOG.info(json);
      out.output(json);
    }
  }

  /**
   * Process imbalance records.
   */
  static class ParseImbalance extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<String> out) {
      // Use OutputReceiver.output to emit the output element.

      String[] lineArray = row.split(",");
      ImbalanceRecord a = new ImbalanceRecord(lineArray);
      String json = a.toJSON();
      //LOG.info(json);
      out.output(json);
    }
  }

  /**
   * This was the original function to process the A records only.
   */
  static class ParseNyseARecords extends DoFn<String, String> {
    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<String> out) {
      // Use OutputReceiver.output to emit the output element.
      String[] lineArray = row.split(",");
      String messageType = lineArray[0].trim();
      switch(messageType){
        case "A": { //add order
          AddOrderRecord a = new AddOrderRecord(lineArray);
          String json = a.toJSON();
          LOG.info(json);
          out.output(json);
          break;
        }
        case "M":{ // modify order
          ModifyRecord m = new ModifyRecord(lineArray);
          String json = m.toJSON();
          LOG.info(json);
          out.output(json);
          break;
        }
        case "D": { //delete order
          break;
        }
        case "I": { // imbalance message
          break;
        }
        case "V": {// system event message
          break;
        }
      }
    }
  }

  /**
   * This is a static class that is used to inject additional data into the record.
   * TODO - convert to dynamic sideInput so users can pass this info via a command line option and source file.
   */
  static abstract class NyseRecord {
    String message_type;
    String message_type_descr;
    String sequence_number;
    String order_reference_number;
    String exchange_code;
    String exchange_code_descr;
    String buy_sell_indicator;
    String buy_sell_indicator_descr;
    int shares;
    String stock_symbol;
    float price;
    String date;
    String system_code;
    String system_code_descr;
    String quote_id;
    String quote_id_descr;
    //specific to system message
    String event_code;
    String event_code_descr;
    String expected_sequence_number;
    //specific to Imbalance message
    String auction_type;
    String auction_type_descr;
    String auction_time;
    float market_imbalance;
    float total_imbalance;

    public NyseRecord(String[] fields) {
      this(fields[0].charAt(0), fields);
      message_type = fields[0];
    }

    public NyseRecord(char recordType, String[] fields){
      switch(recordType){
        case 'A':
          sequence_number = fields[1];
          order_reference_number = fields[2];
          exchange_code = fields[3];
          exchange_code_descr = addOrderExchangeCodeDescr(fields[3]);
          buy_sell_indicator = fields[4];
          buy_sell_indicator_descr = addOrderBuySellCodeDescr(fields[4]);
          shares = Integer.parseInt(fields[5]);
          stock_symbol = fields[6];
          price = Float.parseFloat(fields[7]);
          date = parseDate(Integer.parseInt(fields[8]), Integer.parseInt(fields[9]));
          system_code = fields[10];
          system_code_descr = systemCodeDescr(fields[10]);
          quote_id = fields[11];
          quote_id_descr = quoteIdDescr(fields[11]);
          break;
        case 'M':
          sequence_number = fields[1];
          order_reference_number = fields[2];
          shares = Integer.parseInt(fields[3]);
          price = Float.parseFloat(fields[4]);
          date = parseDate(Integer.parseInt(fields[5]), Integer.parseInt(fields[6]));
          stock_symbol = fields[7];
          exchange_code = fields[8];
          exchange_code_descr = addOrderExchangeCodeDescr(fields[8]);
          system_code = fields[9];
          system_code_descr = systemCodeDescr(fields[9]);
          quote_id = fields[10];
          quote_id_descr = quoteIdDescr(fields[10]);
          buy_sell_indicator = fields[11];
          buy_sell_indicator_descr = addOrderBuySellCodeDescr(fields[11]);
          break;
        case 'D':
          sequence_number = fields[1];
          order_reference_number = fields[2];
          date = parseDate(Integer.parseInt(fields[3]), Integer.parseInt(fields[4]));
          stock_symbol = fields[5];
          exchange_code = fields[6];
          exchange_code_descr = addOrderExchangeCodeDescr(fields[6]);
          system_code = fields[7];
          system_code_descr = systemCodeDescr(fields[7]);
          quote_id = fields[8];
          quote_id_descr = quoteIdDescr(fields[8]);
          buy_sell_indicator = fields[9];
          buy_sell_indicator_descr = addOrderBuySellCodeDescr(fields[9]);
          break;
        case 'I':
          sequence_number = fields[1];
          stock_symbol = fields[2];
          price = Float.parseFloat(fields[3]);
          shares = Integer.parseInt(fields[4]);
          date = parseDate(Integer.parseInt(fields[6]), Integer.parseInt(fields[7]));
          exchange_code = fields[11];
          exchange_code_descr = addOrderExchangeCodeDescr(fields[11]);
          system_code = fields[12];
          system_code_descr = systemCodeDescr(fields[12]);
          break;
        case 'V':
          sequence_number = fields[1];
          date = parseDate(Integer.parseInt(fields[4]), Integer.parseInt(fields[5]));
          system_code = fields[7];
          system_code_descr = systemCodeDescr(fields[7]);
          stock_symbol = fields[8];
          break;
          default:
      }
    }

    static String addOrderExchangeCodeDescr(String code){
      switch(code){
        case "N":
          return "NYSE";
        case "P":
          return "NYSE Arca";
        case "B":
          return "NYSE Arca BB";
      }
      return null;
    }

    static String addOrderBuySellCodeDescr(String code){
      switch(code){
        case "B":
          return "Buy Order";
        case "S":
          return "Sell Order";
        default:
          return null;
      }
    }

    static String parseDate(int seconds, int milliseconds){
      LocalDate myDate = LocalDate.parse("2013-04-03");
      return myDate.toString();
    }

    static String systemCodeDescr(String code){
      switch(code){
        case "L":
          return "Listed";
        case "O":
          return "OTC";
        case "E":
          return "ETR";
        case "B":
          return "BB";
        default:
          return null;
      }
    }

    static String quoteIdDescr(String code){
      switch(code){
        case "A+MPID":
          return "attributed";
        case "AARCA":
          return "non-attributed";
        default:
          return null;
      }
    }

    String toJSON(){
      return gson.toJson(this);
    }
  }

  /**
   * Add order message
   */
  static class AddOrderRecord extends NyseRecord { ;
    public AddOrderRecord(String[] row) {
      super(row);
      message_type_descr = "Add a new order";
    }
  }

  /**
   * Modify order message
   */
  static class ModifyRecord extends NyseRecord { ;
    public ModifyRecord(String[] row) {
      super(row);
      message_type_descr = "Modify order";
    }
  }

  /**
   * Delete order message
   */
  static class DeleteRecord extends NyseRecord { ;
    public DeleteRecord(String[] row) {
      super(row);
      message_type_descr = "Delete order";
    }
  }

  /**
   * Imbalance message
   */
  static class ImbalanceRecord extends NyseRecord { ;
    public ImbalanceRecord(String[] fields) {
      super(fields);
      message_type_descr = "Imbalance message";
      total_imbalance = Float.parseFloat(fields[5]);
      market_imbalance = Float.parseFloat(fields[8]);
      auction_type = fields[9];
      auction_type_descr = auctionTypeDescr(fields[9]);
      int hour = parseHour(fields[10]);
      int minute = parseMinute(fields[10]);
      auction_time = new LocalDateTime(YEAR, MONTH, DAY, hour, minute).toString();
    }

      /**
       * Returns the hour
       * @param time
       * @return
       */
    int parseHour(String time){
        if(time.length() == 4) { //HHMM
            return Integer.parseInt(time.substring(0,2));
        } else if (time.length() == 3){
            return Integer.parseInt(time.substring(0,1));
        } else return 0;
    }

    int parseMinute(String time){
        if(time.length() == 4) { //HHMM
            return Integer.parseInt(time.substring(2,4));
        } else if (time.length() == 3){
            return Integer.parseInt(time.substring(1,3));
        } else return 0;
    }

    String auctionTypeDescr(String code){
      switch(code){
        case "O":
          return "Open";
        case "M":
          return "Market";
        case "H":
          return "Halt";
        case "C":
          return "Closing";
        default:
          return null;
      }
    }
  }

  static class SystemEventRecord extends NyseRecord { ;
    public SystemEventRecord(String[] fields) {
      super(fields);
      message_type_descr = "System event";
      expected_sequence_number = fields[2];
      event_code = fields[5];
      event_code_descr = eventCodeDescr(fields[5]);
    }

    String eventCodeDescr(String code){
      switch(code){
        case "S":
          return "Clear book by symbol";
        default:
          return null;
      }
    }
  }
}

