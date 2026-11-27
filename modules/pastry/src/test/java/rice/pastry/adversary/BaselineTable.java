package rice.pastry.adversary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The recorded baseline: cells of runs summarised over their seeds (mean and 5th percentile), written as one JSON line
 * per cell and as a Markdown table, and read back from the test resources so later phases compare against it.
 */
public final class BaselineTable {

  /** The resource of the full matrix (5 seeds per cell), recorded on the code before any fix. */
  public static final String MATRIX_RESOURCE = "/rice/pastry/adversary/baseline-matrix.jsonl";
  /** The resource of the smoke cells of the default build (1 seed each). */
  public static final String SMOKE_RESOURCE = "/rice/pastry/adversary/baseline-smoke.jsonl";
  /** Where runs write their lines and tables, relative to the module directory. */
  public static final Path OUTPUT_DIRECTORY = Path.of("target", "adversary");

  private static final Pattern TEXT_FIELD = Pattern.compile("\"([A-Za-z0-9]+)\":\"((?:[^\"\\\\]|\\\\.)*)\"");
  private static final Pattern NUMBER_FIELD = Pattern.compile("\"([A-Za-z0-9]+)\":(-?[0-9]+(?:\\.[0-9]+)?)");

  /** The metrics summarised per cell, by name, with how to read each from a run. */
  static final Map<String, ToDoubleFunction<RunMetrics>> SUMMARISED = new LinkedHashMap<>();

  static {
    SUMMARISED.put("deliveryRatio", RunMetrics::deliveryRatio);
    SUMMARISED.put("lookupSuccess", RunMetrics::lookupSuccess);
    SUMMARISED.put("preAttackLookupSuccess", RunMetrics::preAttackLookupSuccess);
    SUMMARISED.put("maliciousShareRoutingTable", RunMetrics::maliciousShareRoutingTable);
    SUMMARISED.put("maliciousShareLeafSet", RunMetrics::maliciousShareLeafSet);
    SUMMARISED.put("routingTableDiversityMean", RunMetrics::routingTableDiversityMean);
    SUMMARISED.put("leafSetDiversityMean", RunMetrics::leafSetDiversityMean);
    SUMMARISED.put("convergedJoinerFraction", RunMetrics::convergedJoinerFraction);
    SUMMARISED.put("convergenceMeanMillis", RunMetrics::convergenceMeanMillis);
    SUMMARISED.put("recoveryMillis", metrics -> metrics.recoveryMillis());
    SUMMARISED.put("honestMessagesPerLookup", RunMetrics::honestMessagesPerLookup);
    SUMMARISED.put("amplification", RunMetrics::amplification);
  }

  private BaselineTable() {}

  /** A cell of the matrix summarised over its seeds. */
  public record Cell(String label, int seedCount, Map<String, Double> means, Map<String, Double> fifthPercentiles) {

    public double mean(String metric) {
      Double value = means.get(metric);
      if (value == null) throw new IllegalArgumentException("no metric " + metric + " in cell " + label);
      return value;
    }

    public double fifthPercentile(String metric) {
      Double value = fifthPercentiles.get(metric);
      if (value == null) throw new IllegalArgumentException("no metric " + metric + " in cell " + label);
      return value;
    }

    String toJsonLine() {
      StringBuilder line = new StringBuilder("{\"cell\":\"").append(label).append("\",\"seeds\":").append(seedCount);
      for (String metric : means.keySet()) {
        line.append(",\"").append(metric).append("Mean\":").append(format(means.get(metric)));
        line.append(",\"").append(metric).append("P5\":").append(format(fifthPercentiles.get(metric)));
      }
      return line.append('}').toString();
    }
  }

  /** Summarises runs of the same cell; the runs' scenarios must differ only in their seed. */
  public static Cell summarise(List<RunMetrics> runs) {
    if (runs.isEmpty()) throw new IllegalArgumentException("no runs");
    Map<String, Double> means = new LinkedHashMap<>();
    Map<String, Double> fifthPercentiles = new LinkedHashMap<>();
    for (Map.Entry<String, ToDoubleFunction<RunMetrics>> metric : SUMMARISED.entrySet()) {
      // -1 marks a value a run does not define; it is left out, except that one run without recovery makes the cell
      // one without recovery
      List<Double> values = new ArrayList<>();
      boolean hasUndefined = false;
      for (RunMetrics run : runs) {
        double value = metric.getValue().applyAsDouble(run);
        if (value < 0) {
          hasUndefined = true;
        } else {
          values.add(value);
        }
      }
      if (hasUndefined && metric.getKey().equals("recoveryMillis")) values.clear();
      means.put(metric.getKey(), Metrics.mean(values));
      fifthPercentiles.put(metric.getKey(), Metrics.percentile5(values));
    }
    return new Cell(runs.get(0).scenario().cellLabel(), runs.size(), means, fifthPercentiles);
  }

  /** The Markdown table of {@code cells}, for the design doc and the PR. */
  public static String markdown(List<Cell> cells) {
    StringBuilder table = new StringBuilder();
    table.append("| Cell | Seeds | Delivery | Lookup success (p5) | Malicious share RT / LS | Diversity RT / LS "
        + "| Joiners converged | Convergence s | Recovery s | Msgs per lookup | Amplification |\n");
    table.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
    for (Cell cell : cells) {
      table.append("| ").append(cell.label())
          .append(" | ").append(cell.seedCount())
          .append(" | ").append(format(cell.mean("deliveryRatio")))
          .append(" | ").append(format(cell.mean("lookupSuccess"))).append(" (")
          .append(format(cell.fifthPercentile("lookupSuccess"))).append(')')
          .append(" | ").append(format(cell.mean("maliciousShareRoutingTable"))).append(" / ")
          .append(format(cell.mean("maliciousShareLeafSet")))
          .append(" | ").append(format(cell.mean("routingTableDiversityMean"))).append(" / ")
          .append(format(cell.mean("leafSetDiversityMean")))
          .append(" | ").append(format(cell.mean("convergedJoinerFraction")))
          .append(" | ").append(seconds(cell.mean("convergenceMeanMillis")))
          .append(" | ").append(seconds(cell.mean("recoveryMillis")))
          .append(" | ").append(String.format(Locale.ROOT, "%.1f", cell.mean("honestMessagesPerLookup")))
          .append(" | ").append(String.format(Locale.ROOT, "%.1f", cell.mean("amplification")))
          .append(" |\n");
    }
    return table.toString();
  }

  /** Writes the cells as JSON lines and as a Markdown table under {@link #OUTPUT_DIRECTORY}. */
  public static void write(String name, List<Cell> cells) throws IOException {
    Files.createDirectories(OUTPUT_DIRECTORY);
    List<String> lines = new ArrayList<>();
    for (Cell cell : cells) lines.add(cell.toJsonLine());
    Files.write(OUTPUT_DIRECTORY.resolve(name + ".jsonl"), lines, StandardCharsets.UTF_8);
    Files.writeString(OUTPUT_DIRECTORY.resolve(name + ".md"), markdown(cells), StandardCharsets.UTF_8);
  }

  /** Appends one run's line to {@code target/adversary/<name>.jsonl}. */
  public static void appendRun(String name, RunMetrics run) throws IOException {
    Files.createDirectories(OUTPUT_DIRECTORY);
    Files.writeString(OUTPUT_DIRECTORY.resolve(name + ".jsonl"), run.toJsonLine() + "\n", StandardCharsets.UTF_8,
        java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
  }

  /**
   * Reads a recorded resource: each line's numeric fields by name, keyed by its {@code cell} field (for a run line,
   * the cell and the seed, as {@code <cell> seed=<seed>}).
   */
  public static Map<String, Map<String, Double>> read(String resource) throws IOException {
    Map<String, Map<String, Double>> rows = new LinkedHashMap<>();
    try (InputStream stream = BaselineTable.class.getResourceAsStream(resource)) {
      if (stream == null) throw new IOException("no resource " + resource);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        if (line.isBlank()) continue;
        String cell = null;
        Matcher text = TEXT_FIELD.matcher(line);
        while (text.find()) {
          if (text.group(1).equals("cell")) cell = text.group(2);
        }
        Map<String, Double> numbers = new LinkedHashMap<>();
        Matcher number = NUMBER_FIELD.matcher(line);
        while (number.find()) numbers.put(number.group(1), Double.parseDouble(number.group(2)));
        if (cell == null) throw new IOException("line without a cell in " + resource + ": " + line);
        Double seed = numbers.get("seed");
        rows.put(seed == null ? cell : cell + " seed=" + seed.longValue(), numbers);
      }
    }
    return rows;
  }

  /** The key {@link #read(String)} gives a run line. */
  public static String runKey(Scenario scenario) {
    return scenario.cellLabel() + " seed=" + scenario.seed();
  }

  private static String format(Double value) {
    return String.format(Locale.ROOT, "%.4f", value);
  }

  private static String seconds(double millis) {
    return millis < 0 ? "n/a" : String.format(Locale.ROOT, "%.1f", millis / 1000);
  }
}
