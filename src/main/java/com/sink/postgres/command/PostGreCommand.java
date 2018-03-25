package com.sink.postgres.command;

public interface PostGreCommand {

    String getCommand();

    String getErrorMessage();
}
