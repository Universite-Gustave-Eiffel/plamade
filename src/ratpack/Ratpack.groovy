import groovy.sql.Sql
import org.flywaydb.core.Flyway
import ratpack.h2.H2Module
import ratpack.service.Service
import ratpack.service.StartEvent

import javax.sql.DataSource

import static ratpack.groovy.Groovy.ratpack
import static ratpack.jackson.Jackson.json

ratpack {

    bindings {

        // Use H2 in-memory database
        module(new H2Module("sa", "", "jdbc:h2:mem:convertions;DB_CLOSE_DELAY=-1"))

        // Migrate flyway scripts on startup
        bindInstance new Service() {
            void onStart(StartEvent event) {
                new Flyway(dataSource: event.registry.get(DataSource)).migrate()
            }
        }


    }

    handlers {

        get {
            render json([
                    "result" : "hello"
            ])
        }
        /*
            GET /convert/EUR/USD/100
         */
        get("test/:from/:to/:amount") {


            // 4: Render a response for a our clients
            render json([
                    "fromCurrency" : pathTokens.from,
                    "toCurrency" : pathTokens.to,
                    "fromAmount": pathTokens.amount.toBigDecimal().stripTrailingZeros(),
                    "toAmount": 500
            ])
        }
    }
}
