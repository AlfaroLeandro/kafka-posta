package com.vinsguru.common.exception;

public class EventAlreadyProcessedException extends RuntimeException{
    private static final String MESSAGE = "The event was already processed";

    public EventAlreadyProcessedException() {
        super(MESSAGE);
    }
}
