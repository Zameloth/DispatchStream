package fr.esgi.kafka.dispatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.junit.jupiter.api.Test;

/**
 * Filet de securite anti-crash (DISP-1 / ADR-0005) : toute exception non
 * rattrapee dans un StreamThread doit entrainer le remplacement du thread
 * (REPLACE_THREAD) plutot que l'arret de l'application entiere
 * (comportement par defaut de Kafka Streams : SHUTDOWN_APPLICATION).
 */
class DispatchUncaughtExceptionHandlerTest {

    private final DispatchUncaughtExceptionHandler handler = new DispatchUncaughtExceptionHandler();

    @Test
    void replaceLeThreadSurRuntimeException() {
        StreamThreadExceptionResponse response = handler.handle(new RuntimeException("boom"));

        assertEquals(StreamThreadExceptionResponse.REPLACE_THREAD, response);
    }

    @Test
    void replaceLeThreadMemeSurUneErrorGrave() {
        StreamThreadExceptionResponse response = handler.handle(new OutOfMemoryError("simulated"));

        assertEquals(StreamThreadExceptionResponse.REPLACE_THREAD, response);
    }
}
