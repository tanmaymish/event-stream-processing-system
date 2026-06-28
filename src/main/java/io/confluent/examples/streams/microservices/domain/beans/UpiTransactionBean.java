package io.confluent.examples.streams.microservices.domain.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * UPI (Unified Payments Interface) transaction event - India's real-time payment network.
 * Handles transactions across all UPI handles: @oksbi, @ybl (PhonePe), @paytm, @okaxis, etc.
 */
public class UpiTransactionBean {

  private String txnId;
  private String senderVpa;       // e.g. rahul.sharma@oksbi
  private String receiverVpa;     // e.g. merchant@okaxis
  private String senderBank;      // SBI, HDFC, ICICI, Axis, PNB, Kotak
  private String receiverBank;
  private double amountInr;       // Amount in Indian Rupees
  private String status;          // SUCCESS, FAILED, PENDING, REVERSED
  private String category;        // Food, Shopping, Transport, Utilities, Rent, Medical, Education
  private String merchantName;    // Swiggy, Zomato, Amazon, Ola, etc. (null for P2P)
  private String merchantId;
  private String senderCity;      // Mumbai, Delhi, Bangalore, etc.
  private String senderState;     // Maharashtra, Karnataka, Delhi NCT, etc.
  private String deviceType;      // ANDROID, IOS, WEB
  private String upiApp;          // PHONEPE, GOOGLEPAY, PAYTM, BHIM, AMAZONPAY
  private long timestamp;
  private String failureReason;   // INSUFFICIENT_BALANCE, INVALID_VPA, BANK_DOWN, etc.

  public UpiTransactionBean() {
  }

  public UpiTransactionBean(String txnId, String senderVpa, String receiverVpa,
      String senderBank, String receiverBank, double amountInr, String status,
      String category, String merchantName, String merchantId, String senderCity,
      String senderState, String deviceType, String upiApp, long timestamp, String failureReason) {
    this.txnId = txnId;
    this.senderVpa = senderVpa;
    this.receiverVpa = receiverVpa;
    this.senderBank = senderBank;
    this.receiverBank = receiverBank;
    this.amountInr = amountInr;
    this.status = status;
    this.category = category;
    this.merchantName = merchantName;
    this.merchantId = merchantId;
    this.senderCity = senderCity;
    this.senderState = senderState;
    this.deviceType = deviceType;
    this.upiApp = upiApp;
    this.timestamp = timestamp;
    this.failureReason = failureReason;
  }

  @JsonProperty("txnId") public String getTxnId() { return txnId; }
  public void setTxnId(String txnId) { this.txnId = txnId; }

  @JsonProperty("senderVpa") public String getSenderVpa() { return senderVpa; }
  public void setSenderVpa(String senderVpa) { this.senderVpa = senderVpa; }

  @JsonProperty("receiverVpa") public String getReceiverVpa() { return receiverVpa; }
  public void setReceiverVpa(String receiverVpa) { this.receiverVpa = receiverVpa; }

  @JsonProperty("senderBank") public String getSenderBank() { return senderBank; }
  public void setSenderBank(String senderBank) { this.senderBank = senderBank; }

  @JsonProperty("receiverBank") public String getReceiverBank() { return receiverBank; }
  public void setReceiverBank(String receiverBank) { this.receiverBank = receiverBank; }

  @JsonProperty("amountInr") public double getAmountInr() { return amountInr; }
  public void setAmountInr(double amountInr) { this.amountInr = amountInr; }

  @JsonProperty("status") public String getStatus() { return status; }
  public void setStatus(String status) { this.status = status; }

  @JsonProperty("category") public String getCategory() { return category; }
  public void setCategory(String category) { this.category = category; }

  @JsonProperty("merchantName") public String getMerchantName() { return merchantName; }
  public void setMerchantName(String merchantName) { this.merchantName = merchantName; }

  @JsonProperty("merchantId") public String getMerchantId() { return merchantId; }
  public void setMerchantId(String merchantId) { this.merchantId = merchantId; }

  @JsonProperty("senderCity") public String getSenderCity() { return senderCity; }
  public void setSenderCity(String senderCity) { this.senderCity = senderCity; }

  @JsonProperty("senderState") public String getSenderState() { return senderState; }
  public void setSenderState(String senderState) { this.senderState = senderState; }

  @JsonProperty("deviceType") public String getDeviceType() { return deviceType; }
  public void setDeviceType(String deviceType) { this.deviceType = deviceType; }

  @JsonProperty("upiApp") public String getUpiApp() { return upiApp; }
  public void setUpiApp(String upiApp) { this.upiApp = upiApp; }

  @JsonProperty("timestamp") public long getTimestamp() { return timestamp; }
  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

  @JsonProperty("failureReason") public String getFailureReason() { return failureReason; }
  public void setFailureReason(String failureReason) { this.failureReason = failureReason; }

  @Override
  public String toString() {
    return "UpiTransaction{txnId='" + txnId + "', sender='" + senderVpa + "', receiver='"
        + receiverVpa + "', amount=₹" + amountInr + ", status='" + status + "', city='"
        + senderCity + "'}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UpiTransactionBean that = (UpiTransactionBean) o;
    return Objects.equals(txnId, that.txnId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(txnId);
  }
}
