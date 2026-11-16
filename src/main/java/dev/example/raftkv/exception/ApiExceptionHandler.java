package dev.example.raftkv.exception;

import dev.example.raftkv.dto.kv.ErrorResponse;
import dev.example.raftkv.enums.ApiErrorCode;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

/** Converts application and request failures into consistent HTTP error responses. */
@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(NotLeaderException.class)
    public ResponseEntity<ErrorResponse> handleNotLeader(NotLeaderException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResponse(
                        ApiErrorCode.NOT_LEADER.value(), exception.getMessage(), exception.getLeader()));
    }

    @ExceptionHandler(WriteCommitTimeoutException.class)
    public ResponseEntity<ErrorResponse> handleWriteCommitTimeout(WriteCommitTimeoutException exception) {
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body(new ErrorResponse(ApiErrorCode.WRITE_COMMIT_TIMEOUT.value(),
                        exception.getMessage(), null));
    }

    @ExceptionHandler(IdempotencyKeyConflictException.class)
    public ResponseEntity<ErrorResponse> handleIdempotencyKeyConflict(
            IdempotencyKeyConflictException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ErrorResponse(ApiErrorCode.IDEMPOTENCY_KEY_REUSED.value(),
                        exception.getMessage(), null));
    }

    @ExceptionHandler(InvalidOrderException.class)
    public ResponseEntity<ErrorResponse> handleInvalidOrder(InvalidOrderException exception) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(ApiErrorCode.INVALID_ORDER.value(), exception.getMessage(), null));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleInvalidBody(MethodArgumentNotValidException exception) {
        String message = exception.getBindingResult().getFieldErrors().stream()
                .map(ApiExceptionHandler::formatFieldError)
                .collect(Collectors.joining("; "));
        return badRequest(ApiErrorCode.VALIDATION_ERROR, message);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(ConstraintViolationException exception) {
        return badRequest(ApiErrorCode.VALIDATION_ERROR, exception.getMessage());
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadableBody(HttpMessageNotReadableException exception) {
        return badRequest(ApiErrorCode.INVALID_REQUEST, "Request body is missing or malformed");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpectedError(Exception exception) {
        log.error("Unhandled API error", exception);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(
                        ApiErrorCode.INTERNAL_ERROR.value(), "Internal server error", null));
    }

    private ResponseEntity<ErrorResponse> badRequest(ApiErrorCode code, String message) {
        return ResponseEntity.badRequest().body(new ErrorResponse(code.value(), message, null));
    }

    private static String formatFieldError(FieldError error) {
        return error.getField() + ": " + error.getDefaultMessage();
    }
}
