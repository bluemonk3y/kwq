package io.confluent.kwq;

public interface Kwq {

  Task consume();

  void pause();
  void start();

  String status();

}
