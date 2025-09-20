package com.tihon.kafkademo.handler;

import com.tihon.kafkademo.exception.UnsuccessfulSendMessageException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class MessageExceptionHandler {

    @ExceptionHandler(UnsuccessfulSendMessageException.class)
    public ResponseEntity<String> unsuccessfulSendMessage(UnsuccessfulSendMessageException e){
        return ResponseEntity.internalServerError().body(e.getMessage());
    }
}