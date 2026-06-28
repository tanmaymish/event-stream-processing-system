package io.confluent.examples.streams.microservices.domain.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Fraud/risk alert raised by UPI stream processing pipelines.
 */
public class UpiAlertBean {

  private String alertId;
  private String alertType;       // VELOCITY_FRAUD, HIGH_VALUE_SPIKE, MULTI_CITY, FAILED_BURST, MULE_ACCOUNT
  private String severity;        // LOW, MEDIUM, HIGH, CRITICAL
  private String senderVpa;
  private String senderBank;
  private String description;
  private long windowStartMs;
  private long windowEndMs;
  private long txnCount;
  private double totalAmountInr;
  private long raisedAt;

  public UpiAlertBean() {
  }

  public UpiAlertBean(String alertId, String alertType, String severity, String senderVpa,
      String senderBank, String description, long windowStartMs, long windowEndMs,
      long txnCount, double totalAmountInr, long raisedAt) {
    this.alertId = alertId;
    this.alertType = alertType;
    this.severity = severity;
    this.senderVpa = senderVpa;
    this.senderBank = senderBank;
    this.description = description;
    this.windowStartMs = windowStartMs;
    this.windowEndMs = windowEndMs;
    this.txnCount = txnCount;
    this.totalAmountInr = totalAmountInr;
    this.raisedAt = raisedAt;
  }

  @JsonProperty("alertId") public String getAlertId() { return alertId; }
  public void setAlertId(String alertId) { this.alertId = alertId; }

  @JsonProperty("alertType") public String getAlertType() { return alertType; }
  public void setAlertType(String alertType) { this.alertType = alertType; }

  @JsonProperty("severity") public String getSeverity() { return severity; }
  public void setSeverity(String severity) { this.severity = severity; }

  @JsonProperty("senderVpa") public String getSenderVpa() { return senderVpa; }
  public void setSenderVpa(String senderVpa) { this.senderVpa = senderVpa; }

  @JsonProperty("senderBank") public String getSenderBank() { return senderBank; }
  public void setSenderBank(String senderBank) { this.senderBank = senderBank; }

  @JsonProperty("description") public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }

  @JsonProperty("windowStartMs") public long getWindowStartMs() { return windowStartMs; }
  public void setWindowStartMs(long windowStartMs) { this.windowStartMs = windowStartMs; }

  @JsonProperty("windowEndMs") public long getWindowEndMs() { return windowEndMs; }
  public void setWindowEndMs(long windowEndMs) { this.windowEndMs = windowEndMs; }

  @JsonProperty("txnCount") public long getTxnCount() { return txnCount; }
  public void setTxnCount(long txnCount) { this.txnCount = txnCount; }

  @JsonProperty("totalAmountInr") public double getTotalAmountInr() { return totalAmountInr; }
  public void setTotalAmountInr(double totalAmountInr) { this.totalAmountInr = totalAmountInr; }

  @JsonProperty("raisedAt") public long getRaisedAt() { return raisedAt; }
  public void setRaisedAt(long raisedAt) { this.raisedAt = raisedAt; }

  @Override
  public String toString() {
    return "UpiAlert{type='" + alertType + "', severity='" + severity + "', vpa='" + senderVpa
        + "', txns=" + txnCount + ", amount=₹" + totalAmountInr + "}";
  }
}
