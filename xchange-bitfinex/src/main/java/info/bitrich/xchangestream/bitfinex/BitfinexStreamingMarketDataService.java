package info.bitrich.xchangestream.bitfinex;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.bitfinex.dto.*;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.marketdata.Trades;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.zip.CRC32;

import static org.knowm.xchange.bitfinex.v1.BitfinexAdapters.*;

/**
 * Created by Lukas Zaoralek on 7.11.17.
 */
public class BitfinexStreamingMarketDataService implements StreamingMarketDataService {
    private static final Logger LOG = LoggerFactory.getLogger(BitfinexStreamingMarketDataService.class);

    private final BitfinexStreamingService service;

    public BitfinexStreamingMarketDataService(BitfinexStreamingService service) {
        this.service = service;
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        String channelName = "book";
        final String depth = args.length > 0 ? args[0].toString() : "100";
        String pair = currencyPair.base.toString() + currencyPair.counter.toString();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return service.subscribeChannel(channelName,
                new Object[]{pair, "P0", depth})
                .scan(new BitfinexOrderbook(new BitfinexOrderbookLevel[0]), (BitfinexOrderbook o, JsonNode s) -> {
                    if ("cs".equals(s.get(1).textValue())) {
                        final OrderBook orderBook = adaptOrderBook(o.toBitfinexDepth(), currencyPair);
                        final int checksum = s.get(2).intValue();

                        final ArrayList<String> csData = new ArrayList<>();

                        for (int i = 0; i < 25; i++) {
                            if (orderBook.getBids().size() > i) {
                                csData.add(orderBook.getBids().get(i).getLimitPrice().toPlainString());
                                csData.add(orderBook.getBids().get(i).getOriginalAmount().toPlainString());
                            }
                            if (orderBook.getAsks().size() > i) {
                                csData.add(orderBook.getAsks().get(i).getLimitPrice().toPlainString());
                                csData.add(orderBook.getAsks().get(i).getOriginalAmount().negate().toPlainString());
                            }
                        }

                        final String csStr = String.join(":", csData);
                        final CRC32 crc32 = new CRC32();
                        crc32.update(csStr.getBytes());
                        final int csCalc = (int) crc32.getValue();

                        if (csCalc != checksum) {
                            if (csData.size() < 100) {
                                System.out.println("Invalid checksum, but less than 100 items");
                            } else {
                                throw new RuntimeException("Invalid checksum " + currencyPair + " " + csCalc + " vs " + checksum);
                            }
                        }

                        return o;
                    }

                    final BitfinexWebSocketOrderbookTransaction bitfinexWebSocketOrderbookTransaction = s.get(1).get(0).isArray() ? mapper.readValue(s.toString(), BitfinexWebSocketSnapshotOrderbook.class) : mapper.readValue(s.toString(), BitfinexWebSocketUpdateOrderbook.class);

                    return bitfinexWebSocketOrderbookTransaction.toBitfinexOrderBook(o);
                })
                .map((BitfinexOrderbook bo) ->adaptOrderBook(bo.toBitfinexDepth(), currencyPair));
    }

    @Override
    public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
        String channelName = "ticker";

        String pair = currencyPair.base.toString() + currencyPair.counter.toString();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        Observable<BitfinexWebSocketTickerTransaction> subscribedChannel = service.subscribeChannel(channelName,
                new Object[]{pair})
                .map(s -> mapper.readValue(s.toString(), BitfinexWebSocketTickerTransaction.class));

        return subscribedChannel
                .map(s -> adaptTicker(s.toBitfinexTicker(), currencyPair));
    }

    @Override
    public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
        String channelName = "trades";
        final String tradeType = args.length > 0 ? args[0].toString() : "te";

        String pair = currencyPair.base.toString() + currencyPair.counter.toString();
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        Observable<BitfinexWebSocketTradesTransaction> subscribedChannel = service.subscribeChannel(channelName,
                new Object[]{pair})
                .filter(s -> s.get(1).asText().equals(tradeType))
                .map(s -> {
                    if (s.get(1).asText().equals("te") || s.get(1).asText().equals("tu")) {
                        return mapper.readValue(s.toString(), BitfinexWebsocketUpdateTrade.class);
                    } else return mapper.readValue(s.toString(), BitfinexWebSocketSnapshotTrades.class);
                });

        return subscribedChannel
                .flatMapIterable(s -> {
                    Trades adaptedTrades = adaptTrades(s.toBitfinexTrades(), currencyPair);
                    return adaptedTrades.getTrades();
                });
    }
}
