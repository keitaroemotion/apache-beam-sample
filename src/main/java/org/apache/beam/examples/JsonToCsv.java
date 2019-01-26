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
import java.util.Set;
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
      Set<String> fields        = json.keySet();
      Collection<Object> values = json.toMap().values(); 

      receiver.output(buildCsvLine(fields, values));
    }
  }

  public static class LineParser extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> jsonLines) {
      PCollection<String> values = jsonLines.apply(ParDo.of(new ParseJsonString()));
      return values;
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

  private static String buildCsvLine(Set<String> fields, Collection<Object> values){
    String fieldString = "";
    String valueString = "";

    for (Object field : fields) {
      fieldString += field.toString() + ",";
    }

    for (Object field : fields) {
      valueString += field.toString() + ",";
    }

    return fieldString + valueString;
  }
}
