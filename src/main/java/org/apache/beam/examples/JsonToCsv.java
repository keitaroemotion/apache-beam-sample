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

public class JsonToCsv {
  static class ExtractWordsFn extends DoFn<String, String> {
    private final Counter emptyLines       = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private final Distribution lineLenDist = Metrics.distribution(
                                               ExtractWordsFn.class,
                                               "lineLenDistro"
                                             );

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      lineLenDist.update(element.length());

      if (element.trim().isEmpty()) {
        emptyLines.inc();
      }

      String[] words = element.split(ExampleUtils.TOKENIZER_PATTERN, -1);

      for (String word : words) {
        if (!word.isEmpty()) {
          receiver.output(word);
        }
      }
    }
  }

  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  public static class CountWords
      extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      PCollection<String> words                = lines.apply(ParDo.of(new ExtractWordsFn()));
      PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

      return wordCounts;
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
     .apply(new CountWords())
     .apply(MapElements.via(new FormatAsTextFn()))
     .apply("WriteCounts", TextIO.write().to(options.getOutput()));
    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    JsonToCsvOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JsonToCsvOptions.class);

    runJsonToCsv(options);
  }
}
