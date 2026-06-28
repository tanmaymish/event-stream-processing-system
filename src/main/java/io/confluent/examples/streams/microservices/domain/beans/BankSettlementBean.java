package io.confluent.examples.streams.microservices.domain.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Tracks net settlement position per bank for NPCI end-of-day clearing.
 * India's NPCI settles UPI transactions in real-time and batch windows.
 */
public class BankSettlementBean {

  private String bankName;      // SBI, HDFC, ICICI, Axis, PNB, Kotak, Yes Bank
  private double totalDebitInr;   // Money going out of this bank's customers
  private double totalCreditInr;  // Money coming into this bank's customers
  private double netPositionInr;  // credit - debit (positive = net receiver)
  private long txnSent;
  private long txnReceived;
  private long txnFailed;
  private long windowStartMs;
  private long windowEndMs;

  public BankSettlementBean() {
  }

  public BankSettlementBean(String bankName, long windowStartMs, long windowEndMs) {
    this.bankName = bankName;
    this.windowStartMs = windowStartMs;
    this.windowEndMs = windowEndMs;
  }

  public BankSettlementBean addDebit(double amount) {
    totalDebitInr += amount;
    txnSent++;
    netPositionInr = totalCreditInr - totalDebitInr;
    return this;
  }

  public BankSettlementBean addCredit(double amount) {
    totalCreditInr += amount;
    txnReceived++;
    netPositionInr = totalCreditInr - totalDebitInr;
    return this;
  }

  public BankSettlementBean addFailed() {
    txnFailed++;
    return this;
  }

  @JsonProperty("bankName") public String getBankName() { return bankName; }
  public void setBankName(String bankName) { this.bankName = bankName; }

  @JsonProperty("totalDebitInr") public double getTotalDebitInr() { return totalDebitInr; }
  public void setTotalDebitInr(double totalDebitInr) { this.totalDebitInr = totalDebitInr; }

  @JsonProperty("totalCreditInr") public double getTotalCreditInr() { return totalCreditInr; }
  public void setTotalCreditInr(double totalCreditInr) { this.totalCreditInr = totalCreditInr; }

  @JsonProperty("netPositionInr") public double getNetPositionInr() { return netPositionInr; }
  public void setNetPositionInr(double netPositionInr) { this.netPositionInr = netPositionInr; }

  @JsonProperty("txnSent") public long getTxnSent() { return txnSent; }
  public void setTxnSent(long txnSent) { this.txnSent = txnSent; }

  @JsonProperty("txnReceived") public long getTxnReceived() { return txnReceived; }
  public void setTxnReceived(long txnReceived) { this.txnReceived = txnReceived; }

  @JsonProperty("txnFailed") public long getTxnFailed() { return txnFailed; }
  public void setTxnFailed(long txnFailed) { this.txnFailed = txnFailed; }

  @JsonProperty("windowStartMs") public long getWindowStartMs() { return windowStartMs; }
  public void setWindowStartMs(long windowStartMs) { this.windowStartMs = windowStartMs; }

  @JsonProperty("windowEndMs") public long getWindowEndMs() { return windowEndMs; }
  public void setWindowEndMs(long windowEndMs) { this.windowEndMs = windowEndMs; }

  @Override
  public String toString() {
    return "BankSettlement{bank='" + bankName + "', debit=₹" + totalDebitInr
        + ", credit=₹" + totalCreditInr + ", net=₹" + netPositionInr + "}";
  }
}
