package com.vinsguru.common.exception;

public class InsufficientBalanceException extends RuntimeException{

    private static final String MESSAGE = "Insufient balance";

    public InsufficientBalanceException() {
        super(MESSAGE);
    }
}
