package io.confluent.examples.streams.microservices.domain.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Real-time merchant revenue and transaction analytics aggregated via Kafka Streams KTable.
 */
public class MerchantStatsBean {

  private String merchantId;
  private String merchantName;
  private String category;
  private long successCount;
  private long failedCount;
  private double totalRevenueInr;
  private double avgTransactionInr;
  private double peakTransactionInr;
  private long lastUpdatedMs;

  public MerchantStatsBean() {
  }

  public MerchantStatsBean(String merchantId, String merchantName, String category) {
    this.merchantId = merchantId;
    this.merchantName = merchantName;
    this.category = category;
    this.successCount = 0;
    this.failedCount = 0;
    this.totalRevenueInr = 0;
    this.avgTransactionInr = 0;
    this.peakTransactionInr = 0;
    this.lastUpdatedMs = System.currentTimeMillis();
  }

  public MerchantStatsBean merge(UpiTransactionBean txn) {
    if ("SUCCESS".equals(txn.getStatus())) {
      successCount++;
      totalRevenueInr += txn.getAmountInr();
      avgTransactionInr = totalRevenueInr / successCount;
      if (txn.getAmountInr() > peakTransactionInr) {
        peakTransactionInr = txn.getAmountInr();
      }
    } else {
      failedCount++;
    }
    lastUpdatedMs = System.currentTimeMillis();
    return this;
  }

  @JsonProperty("merchantId") public String getMerchantId() { return merchantId; }
  public void setMerchantId(String merchantId) { this.merchantId = merchantId; }

  @JsonProperty("merchantName") public String getMerchantName() { return merchantName; }
  public void setMerchantName(String merchantName) { this.merchantName = merchantName; }

  @JsonProperty("category") public String getCategory() { return category; }
  public void setCategory(String category) { this.category = category; }

  @JsonProperty("successCount") public long getSuccessCount() { return successCount; }
  public void setSuccessCount(long successCount) { this.successCount = successCount; }

  @JsonProperty("failedCount") public long getFailedCount() { return failedCount; }
  public void setFailedCount(long failedCount) { this.failedCount = failedCount; }

  @JsonProperty("totalRevenueInr") public double getTotalRevenueInr() { return totalRevenueInr; }
  public void setTotalRevenueInr(double totalRevenueInr) { this.totalRevenueInr = totalRevenueInr; }

  @JsonProperty("avgTransactionInr") public double getAvgTransactionInr() { return avgTransactionInr; }
  public void setAvgTransactionInr(double avgTransactionInr) { this.avgTransactionInr = avgTransactionInr; }

  @JsonProperty("peakTransactionInr") public double getPeakTransactionInr() { return peakTransactionInr; }
  public void setPeakTransactionInr(double peakTransactionInr) { this.peakTransactionInr = peakTransactionInr; }

  @JsonProperty("lastUpdatedMs") public long getLastUpdatedMs() { return lastUpdatedMs; }
  public void setLastUpdatedMs(long lastUpdatedMs) { this.lastUpdatedMs = lastUpdatedMs; }

  @Override
  public String toString() {
    return "MerchantStats{merchant='" + merchantName + "', revenue=₹" + totalRevenueInr
        + ", txns=" + successCount + ", avg=₹" + avgTransactionInr + "}";
  }
}
