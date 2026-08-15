package com.tencent.cloud.mqtt;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.streams.processor.api.Record;
import org.partiql.lang.CompilerPipeline;
import org.partiql.lang.eval.Bindings;
import org.partiql.lang.eval.EvaluationSession;
import org.partiql.lang.eval.ExprValue;
import org.partiql.lang.eval.ExprValueExtensionsKt;
import org.partiql.lang.eval.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.ion.IonException;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;

/**
 * Applies a PartiQL statement to each incoming record. The record value must be
 * JSON text (String or a type whose {@code toString()} yields JSON); it is bound
 * to the global name {@code payload}, so queries read
 * {@code SELECT ... FROM payload WHERE ...}. Returns one output record per result
 * row, or empty when the record is filtered out (or the payload is malformed).
 */
public class SQLTransform<K, V> implements Transform<K, V> {
    private static final Logger log = LoggerFactory.getLogger(SQLTransform.class);

    private final String sql;

    private final IonSystem ion;
    private final Expression expression;

    public SQLTransform(String sql) {
        this.sql = sql;
        this.ion = IonSystemBuilder.standard().build();
        // Compiles the query once; the resulting expression is re-used for every record
        this.expression = CompilerPipeline.standard().compile(sql);
    }

    public String getSql() {
        return sql;
    }

    @Override
    public Optional<List<Record<K, V>>> transform(Record<K, V> record) {
        // JSON is parsed via ion-java; Ion being a superset of JSON, any JSON value is also Ion data
        final IonValue payload;
        try {
            payload = ion.singleValue(record.value().toString().getBytes(StandardCharsets.UTF_8));
        } catch (IonException e) {
            log.warn("Dropping record with malformed JSON payload ({}): {}", e.getMessage(), record.value());
            return Optional.empty();
        }

        final EvaluationSession session = EvaluationSession.builder()
            .globals(
                Bindings.<ExprValue>lazyBindingsBuilder()
                    .addBinding("payload", () -> ExprValue.of(payload))
                    .build())
            .build();

        final ExprValue result = expression.eval(session);

        final List<Record<K, V>> output = new ArrayList<>();
        for (ExprValue row : result) {
            output.add(record.withValue(jsonValue(row)));
        }
        return output.isEmpty() ? Optional.empty() : Optional.of(output);
    }

    // Output rows are JSON text; safe as long as V is String (the only supported value type)
    @SuppressWarnings("unchecked")
    private V jsonValue(ExprValue row) {
        try {
            StringBuilder sb = new StringBuilder();
            try (IonWriter writer = IonTextWriterBuilder.json().build((Appendable) sb)) {
                ExprValueExtensionsKt.toIonValue(row, ion).writeTo(writer);
            }
            return (V) sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize PartiQL result row as JSON", e);
        }
    }
}
