package info.bitrich.xchangestream.bitfinex;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.bitfinex.dto.*;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import io.reactivex.Observable;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.marketdata.Trades;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.HashMap;
import java.util.Map;

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
        final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();

        return service.subscribeChannel(channelName,
                new Object[]{pair, "P0", depth})
                .scan(new BitfinexOrderbook(new BitfinexOrderbookLevel[0]), (BitfinexOrderbook o, JsonNode s) -> {
                    if ("cs".equals(s.get(1).textValue())) {
                        final OrderBook orderBook = adaptOrderBook(o.toBitfinexDepth(), currencyPair);
                        final int checksum = s.get(2).intValue();

                        final ArrayList<BigDecimal> csData = new ArrayList<>();

                        for (int i = 0; i < 25; i++) {
                            if (orderBook.getBids().size() > i) {
                                csData.add(orderBook.getBids().get(i).getLimitPrice());
                                csData.add(orderBook.getBids().get(i).getOriginalAmount());
                            }
                            if (orderBook.getAsks().size() > i) {
                                csData.add(orderBook.getAsks().get(i).getLimitPrice());
                                csData.add(orderBook.getAsks().get(i).getOriginalAmount().negate());
                            }
                        }

                        final String csStr = csData.stream().map(x -> new ChecksumBigDecimalToStringConverter().convert(x)).collect(Collectors.joining(":"));
                        final CRC32 crc32 = new CRC32();
                        crc32.update(csStr.getBytes());
                        final int csCalc = (int) crc32.getValue();

                        if (csCalc != checksum) {
                            throw new RuntimeException("Invalid checksum " + csCalc + " vs " + checksum);
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
        final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();

        Observable<BitfinexWebSocketTickerTransaction> subscribedChannel = service.subscribeChannel(channelName,
                new Object[]{pair})
                .map(s -> mapper.treeToValue(s, BitfinexWebSocketTickerTransaction.class));

        return subscribedChannel
                .map(s -> adaptTicker(s.toBitfinexTicker(), currencyPair));
    }

    @Override
    public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
        String channelName = "trades";
        final String tradeType = args.length > 0 ? args[0].toString() : "te";

        String pair = currencyPair.base.toString() + currencyPair.counter.toString();
        final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();

        Observable<BitfinexWebSocketTradesTransaction> subscribedChannel = service.subscribeChannel(channelName,
                new Object[]{pair})
                .filter(s -> s.get(1).asText().equals(tradeType))
                .map(s -> {
                    if (s.get(1).asText().equals("te") || s.get(1).asText().equals("tu")) {
                        return mapper.treeToValue(s, BitfinexWebsocketUpdateTrade.class);
                    } else return mapper.treeToValue(s, BitfinexWebSocketSnapshotTrades.class);
                });

        return subscribedChannel
                .flatMapIterable(s -> {
                    Trades adaptedTrades = adaptTrades(s.toBitfinexTrades(), currencyPair);
                    return adaptedTrades.getTrades();
                });
    }
}
