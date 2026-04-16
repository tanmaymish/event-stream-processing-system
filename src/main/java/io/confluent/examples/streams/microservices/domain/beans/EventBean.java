package io.confluent.examples.streams.microservices.domain.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Simple DTO for events used by the REST interface.
 */
public class EventBean {

  private String userId;
  private String action;
  private String status;

  public EventBean() {
  }

  public EventBean(String userId, String action, String status) {
    this.userId = userId;
    this.action = action;
    this.status = status;
  }

  @JsonProperty("userId")
  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  @JsonProperty("action")
  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  @JsonProperty("status")
  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventBean eventBean = (EventBean) o;
    return Objects.equals(userId, eventBean.userId) &&
           Objects.equals(action, eventBean.action) &&
           Objects.equals(status, eventBean.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userId, action, status);
  }

  @Override
  public String toString() {
    return "EventBean{" +
           "userId='" + userId + '\'' +
           ", action='" + action + '\'' +
           ", status='" + status + '\'' +
           '}';
  }
}
