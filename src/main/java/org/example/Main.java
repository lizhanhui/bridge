package org.example;

import java.io.ByteArrayInputStream;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import org.partiql.lang.CompilerPipeline;
import org.partiql.lang.eval.Bindings;
import org.partiql.lang.eval.EvaluationSession;
import org.partiql.lang.eval.ExprValue;
import org.partiql.lang.eval.ExprValueExtensionsKt;
import org.partiql.lang.eval.Expression;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    static void main() {
        String jsons = """
                {"id": "1", "name": "person_1", "age": 32, "address": "555 1st street, Seattle", "tags": []}
                {"id": "2", "name": "person_2", "age": 24}
                {"id": "3", "name": "person_3", "age": 25, "address": {"number": 555, "street": "1st street", "city": "Seattle"}, "tags": ["premium_user"]}
                """;

        // Initializes the ion system used by PartiQL
        final IonSystem ion = IonSystemBuilder.standard().build();

        final CompilerPipeline pipeline = CompilerPipeline.standard();

        // Compiles the query, the resulting expression can be re-used to query multiple
        // data sets
        String sql = """
                SELECT payload.name, payload.address, payload.tags
                FROM payload
                WHERE payload.age < 30 and payload.address.number = 555
                """;

        sql = """
                        SELECT *
                        FROM payload
                        WHERE payload.age < 30 and payload.address.number = 555
                        """;
        final Expression selectAndFilter = pipeline.compile(sql);

        // We are using ion-java to parse the JSON data as PartiQL comes with an
        // embedded value factory for
        // Ion data and Ion being a superset of JSON any JSON data is also Ion data
        // http://amzn.github.io/ion-docs/
        // https://github.com/amzn/ion-java
        final IonReader ionReader = IonReaderBuilder.standard().build(new ByteArrayInputStream(jsons.getBytes()));

        // We are using ion-java again to dump the PartiQL query result as JSON
        final IonWriter resultWriter = IonTextWriterBuilder.json().build((Appendable) System.out);
        // Parses all data from the S3 bucket into the Ion DOM
        final IonDatagram values = ion.getLoader().load(ionReader);
        // Evaluation session encapsulates all information to evaluate a PartiQL
        // expression, including the
        // global bindings
        final EvaluationSession session = EvaluationSession.builder()
                        // We implement the Bindings interface using a lambda. Bindings are used to map
                        // names into values,
                        // in this case we are binding the data from the S3 bucket into the
                        // "myS3Document" name
                        .globals(
                                        Bindings.<ExprValue>lazyBindingsBuilder()
                                                        .addBinding("payload", () -> ExprValue.of(values))
                                                        .build())
                        .build();

        // Executes the query in the session that's encapsulating the JSON data
        final ExprValue selectAndFilterResult = selectAndFilter.eval(session);

        // Uses ion-java to dump the result as JSON. It's possible to build your own
        // writer and dump the ExprValue
        // as any format you want.
        ExprValueExtensionsKt.toIonValue(selectAndFilterResult, ion).writeTo(resultWriter);
    }
}
