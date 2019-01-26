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
      Field[] fields            = getFields();
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

  private static String buildCsvLine(Field[] fields, Collection<Object> values){
    String fieldString = "";
    String valueString = "";

    for (Field field : fields) {
      fieldString += field.fieldName + ",";
    }
 
    int i = 0;
    for (Object value : values) {
      String _value = value.toString();

      if (fields[i].type == Field.VARCHAR || fields[i].type == Field.DATETIME){
        _value = String.format("'%s'", _value);
      }

      valueString += _value + ",";
      i++;
    }

    return fieldString + valueString;
  }

  private static class Field{
    public static final int VARCHAR  = 0;
    public static final int DATETIME = 1;
    public static final int NUMBER   = 2;

    public String  fieldName;
    public int type;

    private Field(String _fieldName, int _type){
      fieldName = _fieldName;
      type      = _type;
    }
  }

  private static Field[] getFields(){
    return new Field[]{
             new Field("PAYMENT_TYPE",             Field.VARCHAR),     
             new Field("STORE_AND_FWD_FLAG",       Field.VARCHAR),     
             new Field("FARE_AMOUNT",              Field.NUMBER),
             new Field("PICKUP_LATITUDE",          Field.NUMBER),
             new Field("DROPOFF_DATETIME",         Field.DATETIME),
             new Field("PICKUP_DATETIME",          Field.DATETIME),
             new Field("PICKUP_LONGITUDE",         Field.NUMBER),
             new Field("TIP_AMOUNT",               Field.NUMBER),
             new Field("UUID",                     Field.VARCHAR),
             new Field("TRIP_TYPE",                Field.VARCHAR),
             new Field("RATE_CODE",                Field.VARCHAR),
             new Field("TOLLS_AMOUNT",             Field.NUMBER),
             new Field("DROPOFF_LATITUDE",         Field.NUMBER),
             new Field("DROPOFF_LONGITUDE",        Field.NUMBER),
             new Field("TIME_BETWEEN_SERVICE",     Field.VARCHAR),
             new Field("PASSENGER_COUNT",          Field.NUMBER),
             new Field("DISTANCE_BETWEEN_SERVICE", Field.NUMBER),
             new Field("TOTAL_AMOUNT",             Field.NUMBER),
      };
  }
}
