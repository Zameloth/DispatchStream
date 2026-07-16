package fr.esgi.kafka.dispatch;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * Filet de securite anti-crash pour DISP-1 (voir docs/adr/0005-crash-safety-net-for-disp-1.md).
 *
 * <p>Par defaut, Kafka Streams tue le StreamThread fautif sur toute exception
 * non rattrapee, ce qui entraine l'arret de toute l'application
 * (SHUTDOWN_APPLICATION). Le critere de notation de DISP-1 est un uptime
 * empirique (10 min sans crash sur le cluster partage) : on remplace donc le
 * thread plutot que de le laisser tuer l'application.
 *
 * <p>Ce bean est expose via CDI (@ApplicationScoped) et automatiquement
 * repris par l'extension quarkus-kafka-streams : KafkaStreamsProducer
 * injecte un {@code Instance<StreamsUncaughtExceptionHandler>} et, s'il est
 * satisfait, appelle {@code KafkaStreams#setUncaughtExceptionHandler(...)}
 * avant de demarrer les threads. Aucune configuration supplementaire n'est
 * necessaire.
 */
@ApplicationScoped
public class DispatchUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        Log.errorf(exception,
                "[dispatch] Exception non rattrapee dans un StreamThread : remplacement du thread (ADR-0005)");
        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
