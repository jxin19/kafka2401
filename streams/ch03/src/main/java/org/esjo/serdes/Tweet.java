package org.esjo.serdes;

import com.google.gson.annotations.SerializedName;

public class Tweet {

  @SerializedName("CreatedAt")
  private Long createdAt;

  @SerializedName("Id")
  private String id;

  @SerializedName("Lang")
  private String lang;

  @SerializedName("Retweet")
  private Boolean retweet;

  @SerializedName("Text")
  private String text;

  public Long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Long createdAt) {
    this.createdAt = createdAt;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getLang() {
    return lang;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }

  public Boolean getRetweet() {
    return retweet;
  }

  public void setRetweet(Boolean retweet) {
    this.retweet = retweet;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public void translate() {
    this.lang = "ko";
  }
}
