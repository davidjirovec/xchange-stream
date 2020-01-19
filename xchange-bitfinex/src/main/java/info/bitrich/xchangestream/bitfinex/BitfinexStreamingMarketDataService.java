package info.bitrich.xchangestream.bitfinex;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbook;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbookLevel;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbookSide;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketSnapshotTrades;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketTickerTransaction;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketTradesTransaction;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebsocketUpdateTrade;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import io.reactivex.Observable;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.SortedMap;
import org.knowm.xchange.bitfinex.service.BitfinexAdapters;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.dto.trade.LimitOrder;

import java.math.BigDecimal;
import java.util.Date;

import static io.vavr.API.Tuple;
import static org.knowm.xchange.bitfinex.service.BitfinexAdapters.adaptTicker;
import static org.knowm.xchange.bitfinex.service.BitfinexAdapters.adaptTrades;

/**
 * Created by Lukas Zaoralek on 7.11.17.
 */
public class BitfinexStreamingMarketDataService implements StreamingMarketDataService {

    private final BitfinexStreamingService service;

    public BitfinexStreamingMarketDataService(BitfinexStreamingService service) {
        this.service = service;
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        String channelName = "book";
        final String depth = args.length > 0 ? args[0].toString() : "100";
        final String pair = BitfinexAdapters.adaptCurrencyPair(currencyPair).substring(1);
        final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();

        Observable<List<BitfinexOrderbookLevel>> subscribedChannel = service.subscribeChannel(channelName,
                new Object[]{pair, "P0", depth})
                .map(s ->
                        mapper.readValue(
                                mapper.treeAsTokens(
                                        s.get(1).get(0).isArray() ? s.get(1) : mapper.createArrayNode().add(s.get(1))
                                ),
                                mapper.getTypeFactory()
                                        .constructCollectionLikeType(List.class, BitfinexOrderbookLevel.class)
                        )
                );

        return subscribedChannel
                .scan(
                        new BitfinexOrderbook(),
                        (bitfinexOrderbook, bitfinexOrderbookLevels) -> {
                            final Date date = new Date();
                            final Tuple2<SortedMap<BigDecimal, LimitOrder>, SortedMap<BigDecimal, LimitOrder>> asksBids
                                    = updateAsksBids(bitfinexOrderbookLevels, bitfinexOrderbook, currencyPair, date);
                            return new BitfinexOrderbook(
                                    getSide(asksBids._1, bitfinexOrderbook.getAsks()),
                                    getSide(asksBids._2, bitfinexOrderbook.getBids()),
                                    date
                            );
                        }
                )
                .skip(1)
                .map(bitfinexOrderbook ->
                        new OrderBook(
                                bitfinexOrderbook.getDate(),
                                bitfinexOrderbook.getAsks().getList(),
                                bitfinexOrderbook.getBids().getList()
                        )
                );
    }

    private Tuple2<SortedMap<BigDecimal, LimitOrder>, SortedMap<BigDecimal, LimitOrder>> updateAsksBids(
            final List<BitfinexOrderbookLevel> bitfinexOrderbookLevels,
            final BitfinexOrderbook bitfinexOrderbook,
            final CurrencyPair currencyPair,
            final Date date
    ) {
        return bitfinexOrderbookLevels.foldLeft(
                Tuple(bitfinexOrderbook.getAsks().getSortedMap(), bitfinexOrderbook.getBids().getSortedMap()),
                (asksBids, level) -> level.amount.signum() < 0
                        ? asksBids.map1(sortedMap -> update(sortedMap, level, currencyPair, Order.OrderType.ASK, date))
                        : asksBids.map2(sortedMap -> update(sortedMap, level, currencyPair, Order.OrderType.BID, date))
        );
    }

    private BitfinexOrderbookSide getSide(
            final SortedMap<BigDecimal, LimitOrder> sortedMap, final BitfinexOrderbookSide bitfinexOrderbookSide
    ) {
        return sortedMap == bitfinexOrderbookSide.getSortedMap()
                ? bitfinexOrderbookSide
                : new BitfinexOrderbookSide(sortedMap, sortedMap.values().asJava());
    }

    private SortedMap<BigDecimal, LimitOrder> update(
            final SortedMap<BigDecimal, LimitOrder> sortedMap,
            final BitfinexOrderbookLevel bitfinexOrderbookLevel,
            final CurrencyPair currencyPair,
            final Order.OrderType orderType,
            final Date date
    ) {
        return bitfinexOrderbookLevel.count.signum() == 0
                ? sortedMap.remove(bitfinexOrderbookLevel.price)
                : sortedMap.put(
                        bitfinexOrderbookLevel.price,
                        BitfinexAdapters.adaptOrder(
                                bitfinexOrderbookLevel.amount.abs(),
                                bitfinexOrderbookLevel.price,
                                currencyPair,
                                orderType,
                                date
                        )
                    );
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
