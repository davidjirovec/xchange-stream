package info.bitrich.xchangestream.bitfinex;

import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.disposables.Disposable;
import org.knowm.xchange.ExchangeSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Lukas Zaoralek on 7.11.17.
 */
public class BitfinexManualExample {
    private static final Logger LOG = LoggerFactory.getLogger(BitfinexManualExample.class);

    public static void main(String[] args) {
        final ExchangeSpecification bitfinexSpecification = new ExchangeSpecification(BitfinexStreamingExchange.class);
        bitfinexSpecification.setShouldLoadRemoteMetaData(true);
        final BitfinexStreamingExchange bitfinexExchange = ((BitfinexStreamingExchange) StreamingExchangeFactory.INSTANCE.createExchange(bitfinexSpecification));
        bitfinexExchange.connect().blockingAwait();

        final List<Disposable> collect = bitfinexExchange.getExchangeSymbols().stream().limit(250).map(currencyPair -> bitfinexExchange.getStreamingMarketDataService().getOrderBook(currencyPair, 25).subscribe(orderBook -> {
//            LOG.info("First ask: {}", orderBook.getAsks().get(0));
//            LOG.info("First bid: {}", orderBook.getBids().get(0));
        })).collect(Collectors.toList());


        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
