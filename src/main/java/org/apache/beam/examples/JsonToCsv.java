package org.apache.beam.examples;

/*
 *
 * XXX Need to remove imports not used later
 *
 */
  
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
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
      JSONObject obj = new JSONObject(element);

      Set<String> fields        = obj.keySet();
      Map<String, Object> map   = obj.toMap();
      Collection<Object> values = map.values(); 

      String f = "";
      for (Object field : fields) {
        f += field.toString() + ",";
      }

      String v = "";
      for (Object value : values) {
        v += value.toString() + ",";
      }

      String line = f + v;

      receiver.output(line);
    }
  }

  public static class FormatAsTextFn extends SimpleFunction<KV<String, String>, String> {
    @Override
    public String apply(KV<String, String> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  public static class LineParser
      extends PTransform<PCollection<String>, PCollection<String>> {
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
}
