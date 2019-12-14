package info.bitrich.xchangestream.bitfinex;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbookLevel;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketSnapshotOrderbook;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketSnapshotTrades;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketTickerTransaction;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketTradesTransaction;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketUpdateOrderbook;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebsocketUpdateTrade;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.vavr.API;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.TreeMap;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.knowm.xchange.bitfinex.service.BitfinexAdapters;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.dto.trade.LimitOrder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.zip.CRC32;

import static java.math.BigDecimal.ZERO;
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

    private Tuple2<TreeMap<BigDecimal, LimitOrder>, TreeMap<BigDecimal, LimitOrder>> createFromLevels(
            BitfinexOrderbookLevel[] levels, CurrencyPair currencyPair
    ) {
        final Tuple2<List<BitfinexOrderbookLevel>, List<BitfinexOrderbookLevel>> partition = List.of(levels)
                .filter(level -> level.getCount().compareTo(ZERO) != 0)
                .partition(level -> level.getAmount().compareTo(ZERO) > 0);
        return API.Tuple(
                ((TreeMap)
                        partition._2
                                .toSortedMap(
                                        BitfinexOrderbookLevel::getPrice,
                                        level -> BitfinexAdapters.adaptOrder(
                                                level.getAmount().abs(),
                                                level.getPrice(),
                                                currencyPair,
                                                Order.OrderType.ASK,
                                                null
                                        )
                                )
                ),
                ((TreeMap)
                        partition._1
                                .toSortedMap(
                                        Collections.reverseOrder(),
                                        BitfinexOrderbookLevel::getPrice,
                                        level -> BitfinexAdapters.adaptOrder(
                                                level.getAmount(),
                                                level.getPrice(),
                                                currencyPair,
                                                Order.OrderType.ASK, null
                                        )
                                )
                )
        );
    }

    public Tuple2<TreeMap<BigDecimal, LimitOrder>, TreeMap<BigDecimal, LimitOrder>> withLevel(
            Tuple2<TreeMap<BigDecimal, LimitOrder>, TreeMap<BigDecimal, LimitOrder>> old,
            BitfinexOrderbookLevel level, CurrencyPair currencyPair
    ) {
        final boolean isAsk = level.getAmount().compareTo(ZERO) < 0;

        final TreeMap<BigDecimal, LimitOrder> toModify = isAsk ? old._1 : old._2;

        final TreeMap<BigDecimal, LimitOrder> updatedSide = level.getCount().compareTo(ZERO) != 0
                ? toModify.put(
                        level.getPrice(),
                        BitfinexAdapters.adaptOrder(
                                level.getAmount().abs(),
                                level.getPrice(),
                                currencyPair,
                                isAsk ? Order.OrderType.ASK : Order.OrderType.BID,
                                null
                        )
                )
                : toModify.remove(level.getPrice());

        return isAsk ? API.Tuple(updatedSide, old._2) : API.Tuple(old._1, updatedSide);
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        String channelName = "book";
        final String depth = args.length > 0 ? args[0].toString() : "100";
        final String pair = BitfinexAdapters.adaptCurrencyPair(currencyPair).substring(1);
        final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();

        final Observable<
                ImmutableTriple<
                        Tuple2<TreeMap<BigDecimal, LimitOrder>, TreeMap<BigDecimal, LimitOrder>>, Integer, OrderBook
                        >
                > subscribedChannel
                = service
                .subscribeChannel(channelName, new Object[]{pair, "P0", depth})
                .observeOn(Schedulers.computation())
                .scan(
                        ImmutableTriple.of(
                                API.Tuple(
                                        TreeMap.<BigDecimal, LimitOrder>empty(),
                                        TreeMap.<BigDecimal, LimitOrder>empty(Collections.reverseOrder())
                                ),
                                (Integer) null,
                                (OrderBook) null
                        ),
                        (immutableTriple, jsonNode) -> {
                            if ("cs".equals(jsonNode.get(1).asText())) {
                                return ImmutableTriple.of(
                                        immutableTriple.getLeft(), jsonNode.get(2).asInt(), immutableTriple.getRight()
                                );
                            } else {
                                final Tuple2<TreeMap<BigDecimal, LimitOrder>, TreeMap<BigDecimal, LimitOrder>> tuple2
                                        = jsonNode.get(1).size() == 0 || jsonNode.get(1).get(0).isArray()
                                        ? createFromLevels(
                                                mapper.treeToValue(jsonNode, BitfinexWebSocketSnapshotOrderbook.class)
                                                        .levels,
                                                currencyPair
                                       )
                                        : withLevel(
                                                immutableTriple.getLeft(),
                                                mapper.treeToValue(jsonNode, BitfinexWebSocketUpdateOrderbook.class).getLevel(),
                                                currencyPair
                                       );
                                final OrderBook orderBook = new OrderBook(
                                        null, tuple2._1.values().toJavaStream(), tuple2._2.values().toJavaStream()
                                );
                                return ImmutableTriple.of(tuple2, null, orderBook);
                            }
                        }
                )
                .share();

        final Disposable checksumDisposable = subscribedChannel.filter(triple -> triple.getMiddle() != null)
                .subscribe(triple -> {
                    final int checksum = triple.getMiddle();
                    final OrderBook orderBook = triple.getRight();

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

                    final String csStr = new ChecksumBigDecimalToStringConverter().convert(csData);
                    final CRC32 crc32 = new CRC32();
                    crc32.update(csStr.getBytes());
                    final int csCalc = (int) crc32.getValue();

                    if (csCalc != checksum) {
                        final String msg = "Invalid checksum " + csCalc + " vs " + checksum + " csStr " + csStr
                                + " csData " + csData + " for " + currencyPair;
                        throw new RuntimeException(msg);
                    }
                });

        return subscribedChannel.filter(triple -> triple.getMiddle() == null)
                .map(ImmutableTriple::getRight)
                .doOnDispose(checksumDisposable::dispose);
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
