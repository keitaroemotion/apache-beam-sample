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
    private final Counter emptyLines       = Metrics.counter(ParseJsonString.class, "emptyLines");
    private final Distribution lineLenDist = Metrics.distribution(
                                               ParseJsonString.class,
                                               "lineLenDistro"
                                             );

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      JSONObject obj = new JSONObject(element);

      Set<String> fields        = obj.keySet();
      Map<String, Object> map   = obj.toMap();
      Collection<Object> values = map.values(); 

      for (Object value : values) {
        receiver.output(value.toString());
      }
    }
  }

  public static class FormatAsTextFn extends SimpleFunction<KV<String, String>, String> {
    @Override
    public String apply(KV<String, String> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  public static class LineParser
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> jsonLines) {
      PCollection<String> values                 = jsonLines.apply(ParDo.of(new ParseJsonString()));
      PCollection<KV<String, String>> wordCounts = values.apply(perElement());

      return wordCounts;
    }
  }

  public static <T> PTransform<PCollection<T>, PCollection<KV<T, String>>> perElement() {
    return new PerElement<>();
  }

  private static class PerElement<T> extends PTransform<PCollection<T>, PCollection<KV<T, String>>> {

    private PerElement() {}

    @Override
    public PCollection<KV<T, String>> expand(PCollection<T> input) {
      return input
          .apply(
              "Init",
              MapElements.via(
                  new SimpleFunction<T, KV<T, Void>>() {
                    @Override
                    public KV<T, Void> apply(T element) {
                      return KV.of(element, (Void) null);
                    }
                  }))
          .apply(perKey());
    }
  }

  private static class CountFn<T> extends CombineFn<T, String[], String> {
    @Override
    public String[] createAccumulator() {
      return new String[] {""};
    }

    @Override
    public String[] addInput(String[] accumulator, T input) {
      // accumulator[0] += "";
      // return accumulator;
      return new String[] {""};
    }

    @Override
    public String extractOutput(String[] accumulator) {
      return "accumulator[0]";
    }
    @Override
    public String[] mergeAccumulators(Iterable<String[]> accumulators) {
      return new String[]{""};
    }
  }

  public static <K, V> PTransform<PCollection<KV<K, V>>, PCollection<KV<K, String>>> perKey() {
    return Combine.perKey(new CountFn<V>());
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
     .apply(MapElements.via(new FormatAsTextFn()))
     .apply("WriteCSV", TextIO.write().to(options.getOutput()));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    JsonToCsvOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JsonToCsvOptions.class);

    runJsonToCsv(options);
  }
}
