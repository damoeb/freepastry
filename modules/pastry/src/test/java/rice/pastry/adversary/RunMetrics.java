package rice.pastry.adversary;

import java.util.Locale;

/**
 * The metrics of one run (design section 4, "Metrics"). Ratios are in [0, 1]; a value of -1 means "not defined in
 * this run" (no malicious messages, no converged joiner, no recovery within the window, byte estimate off).
 *
 * @param scenario the run
 * @param preAttackDeliveryRatio lookups delivered to the oracle's root of the key / lookups sent, pre-attack window
 * @param preAttackLookupSuccess lookups answered with the oracle's root within the deadline / lookups sent, pre-attack
 * @param deliveryRatio as {@code preAttackDeliveryRatio}, attack window
 * @param lookupSuccess as {@code preAttackLookupSuccess}, attack window
 * @param lookupsSent lookups sent by honest nodes in the attack window
 * @param maliciousShareRoutingTable malicious entries / entries, over the routing tables of all honest nodes, at the
 *     end of the attack window; compare to {@code f}
 * @param maliciousShareLeafSet the same for the leaf sets
 * @param routingTableDiversityMean Shannon entropy over failure domains of a routing table, normalised by
 *     {@code log2(min(K, entries))}, mean over honest nodes
 * @param routingTableDiversityP5 the same, 5th percentile
 * @param leafSetDiversityMean the same for the leaf sets, mean
 * @param leafSetDiversityP5 the same for the leaf sets, 5th percentile
 * @param convergedJoinerFraction late honest joiners whose leaf set equalled the oracle's before the end / late honest
 *     joiners that stayed
 * @param convergenceMeanMillis mean time from join start to a correct leaf set, over converged joiners
 * @param convergenceMaxMillis the longest of these
 * @param recoveryMillis time from the attack start (the event) until the delivery ratio of every later bucket is back
 *     at 99 % or more of its pre-attack value; 0 if it never dropped, -1 if it is still lower in the last bucket
 * @param honestMessagesPerLookup messages sent by honest nodes in the attack window / lookups sent
 * @param honestBytesPerLookup estimated bytes sent by honest nodes in the attack window / lookups sent
 * @param amplification messages sent by honest nodes / messages sent by malicious nodes, attack window
 * @param droppedMessages routed messages dropped by malicious nodes
 * @param misroutedMessages routed messages misrouted by malicious nodes
 * @param notReadyAtAttackStart nodes of the initial ring that were not ready at the attack start
 */
public record RunMetrics(
    Scenario scenario,
    double preAttackDeliveryRatio,
    double preAttackLookupSuccess,
    double deliveryRatio,
    double lookupSuccess,
    long lookupsSent,
    double maliciousShareRoutingTable,
    double maliciousShareLeafSet,
    double routingTableDiversityMean,
    double routingTableDiversityP5,
    double leafSetDiversityMean,
    double leafSetDiversityP5,
    double convergedJoinerFraction,
    double convergenceMeanMillis,
    double convergenceMaxMillis,
    long recoveryMillis,
    double honestMessagesPerLookup,
    double honestBytesPerLookup,
    double amplification,
    long droppedMessages,
    long misroutedMessages,
    int notReadyAtAttackStart) {

  /** One JSON object on one line; fixed formatting, so equal runs give byte-identical lines. */
  public String toJsonLine() {
    Scenario s = scenario;
    return "{"
        + text("cell", s.cellLabel()) + ","
        + "\"seed\":" + s.seed() + ","
        + "\"nodes\":" + s.nodeCount() + ","
        + number("f", s.maliciousFraction()) + ","
        + text("behaviours", s.behaviourLabel()) + ","
        + text("placement", s.placement().toString()) + ","
        + text("churn", s.churnLabel()) + ","
        + "\"domainOutage\":" + s.hasDomainOutageAtAttackStart() + ","
        + "\"domains\":" + s.domainCount() + ","
        + number("preAttackDeliveryRatio", preAttackDeliveryRatio) + ","
        + number("preAttackLookupSuccess", preAttackLookupSuccess) + ","
        + number("deliveryRatio", deliveryRatio) + ","
        + number("lookupSuccess", lookupSuccess) + ","
        + "\"lookupsSent\":" + lookupsSent + ","
        + number("maliciousShareRoutingTable", maliciousShareRoutingTable) + ","
        + number("maliciousShareLeafSet", maliciousShareLeafSet) + ","
        + number("routingTableDiversityMean", routingTableDiversityMean) + ","
        + number("routingTableDiversityP5", routingTableDiversityP5) + ","
        + number("leafSetDiversityMean", leafSetDiversityMean) + ","
        + number("leafSetDiversityP5", leafSetDiversityP5) + ","
        + number("convergedJoinerFraction", convergedJoinerFraction) + ","
        + number("convergenceMeanMillis", convergenceMeanMillis) + ","
        + number("convergenceMaxMillis", convergenceMaxMillis) + ","
        + "\"recoveryMillis\":" + recoveryMillis + ","
        + number("honestMessagesPerLookup", honestMessagesPerLookup) + ","
        + number("honestBytesPerLookup", honestBytesPerLookup) + ","
        + number("amplification", amplification) + ","
        + "\"droppedMessages\":" + droppedMessages + ","
        + "\"misroutedMessages\":" + misroutedMessages + ","
        + "\"notReadyAtAttackStart\":" + notReadyAtAttackStart
        + "}";
  }

  private static String text(String name, String value) {
    return "\"" + name + "\":\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private static String number(String name, double value) {
    return "\"" + name + "\":" + String.format(Locale.ROOT, "%.4f", value);
  }
}
