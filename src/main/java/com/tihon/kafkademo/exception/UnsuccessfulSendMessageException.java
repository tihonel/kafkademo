package com.tihon.kafkademo.exception;

public class UnsuccessfulSendMessageException extends RuntimeException {
    public UnsuccessfulSendMessageException(String message) {
        super(message);
    }
}