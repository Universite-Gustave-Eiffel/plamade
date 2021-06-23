import groovy.sql.Sql
import ratpack.service.Service
import ratpack.service.StartEvent
import javax.sql.DataSource

import static ratpack.groovy.Groovy.ratpack
import static ratpack.jackson.Jackson.json

ratpack {

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
