package info.bitrich.xchangestream.quoine;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import info.bitrich.xchangestream.quione.QuoineStreamingExchange;
import io.reactivex.disposables.Disposable;
import org.knowm.xchange.currency.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuoineManualExample {
    private static final Logger LOG = LoggerFactory.getLogger(QuoineManualExample.class);

    public static void main(String[] args) {

        StreamingExchange exchange = StreamingExchangeFactory.INSTANCE.createExchange(QuoineStreamingExchange.class.getName());
        exchange.connect().blockingAwait();

        Disposable orderBookDisposable = exchange.getStreamingMarketDataService().getOrderBook(CurrencyPair.BTC_USD).subscribe(orderBook -> {
            LOG.info("First ask: {}", orderBook.getAsks().get(0));
            LOG.info("First bid: {}", orderBook.getBids().get(0));
        });

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        orderBookDisposable.dispose();
        exchange.disconnect().subscribe(() -> LOG.info("Disconnected from the Exchange"));
    }
}
