package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.json.*;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.Iterator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine;

public class JsonToCsv {
  static class ParseJsonString extends DoFn<String, String> {

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      JSONObject json           = new JSONObject(element);
      Set<Table> fields         = getTableFields();
      Collection<Object> values = json.toMap().values(); 

      receiver.output(buildCsvLine(fields, values));
    }
  }

  public static class LineParser extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> jsonLines) {
      return jsonLines.apply(ParDo.of(new ParseJsonString()));
    }
  }

  public interface JsonToCsvOptions extends PipelineOptions {
    @Description("Path of the file to read from")

    String getInputFile();
    void   setInputFile(String value);

    @Description("Path of the file to write to")

    @Required
    String getOutput();

    void setOutput(String value);
  }

  static void runJsonToCsv(JsonToCsvOptions options) {
    Pipeline p = Pipeline.create(options);

    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
     .apply(new LineParser())
     .apply("WriteCSV", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    JsonToCsvOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JsonToCsvOptions.class);

    runJsonToCsv(options);
  }

  private static String buildCsvLine(Set<Table> fields, Collection<Object> values){
    String fieldString = "";
    String valueString = "";

    for (Table field : fields) {
      fieldString += field.fieldName + ",";
    }

    for (Object value : values) {
      valueString += value.toString() + ",";
    }

    return fieldString + valueString;
  }

  private static class Table{
    public static final int VARCHAR  = 0;
    public static final int DATETIME = 1;
    public static final int NUMBER   = 2;

    public String  fieldName;
    public int type;

    private Table(String _fieldName, int _type){
      fieldName = _fieldName;
      type      = _type;
    }
  }

  private static Set<Table> getTableFields(){
    Table[] arrayFields = new Table[]{
                                 new Table("PAYMENT_TYPE",             Table.VARCHAR),     
                                 new Table("STORE_AND_FWD_FLAG",       Table.VARCHAR),     
                                 new Table("FARE_AMOUNT",              Table.VARCHAR),
                                 new Table("PICKUP_LATITUDE",          Table.VARCHAR),
                                 new Table("DROPOFF_DATETIME",         Table.VARCHAR),
                                 new Table("PICKUP_DATETIME",          Table.VARCHAR),
                                 new Table("PICKUP_LONGITUDE",         Table.VARCHAR),
                                 new Table("TIP_AMOUNT",               Table.VARCHAR),
                                 new Table("UUID",                     Table.VARCHAR),
                                 new Table("TRIP_TYPE",                Table.VARCHAR),
                                 new Table("RATE_CODE",                Table.VARCHAR),
                                 new Table("TOLLS_AMOUNT",             Table.VARCHAR),
                                 new Table("DROPOFF_LATITUDE",         Table.VARCHAR),
                                 new Table("DROPOFF_LONGITUDE",        Table.VARCHAR),
                                 new Table("TIME_BETWEEN_SERVICE",     Table.VARCHAR),
                                 new Table("PASSENGER_COUNT",          Table.VARCHAR),
                                 new Table("DISTANCE_BETWEEN_SERVICE", Table.VARCHAR),
                                 new Table("TOTAL_AMOUNT",             Table.VARCHAR),
                           };
    Set<Table> setFields = new HashSet<Table>();
    setFields.addAll(Arrays.asList(arrayFields));   
    return setFields;
  }
}
