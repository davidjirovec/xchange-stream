package info.bitrich.xchangestream.bitfinex;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbook;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexOrderbookLevel;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketAuthBalance;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketAuthOrder;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketAuthPreTrade;
import info.bitrich.xchangestream.bitfinex.dto.BitfinexWebSocketAuthTrade;
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
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.marketdata.Trades;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static org.knowm.xchange.bitfinex.v1.BitfinexAdapters.adaptOrderBook;
import static org.knowm.xchange.bitfinex.v1.BitfinexAdapters.adaptTicker;
import static org.knowm.xchange.bitfinex.v1.BitfinexAdapters.adaptTrades;
import static org.knowm.xchange.bitfinex.v1.BitfinexAdapters.log;

/**
 * Created by Lukas Zaoralek on 7.11.17.
 */
public class BitfinexStreamingMarketDataService implements StreamingMarketDataService {

    private final BitfinexStreamingService service;

    private final Map<CurrencyPair, BitfinexOrderbook> orderbooks = new HashMap<>();

    public BitfinexStreamingMarketDataService(BitfinexStreamingService service) {
        this.service = service;
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        String channelName = "book";
        final String depth = args.length > 0 ? args[0].toString() : "100";
        String pair = currencyPair.base.toString() + currencyPair.counter.toString();
        final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();

        final Observable<ImmutableTriple<BitfinexOrderbook, Integer, OrderBook>> subscribedChannel = service
                .subscribeChannel(channelName, new Object[]{pair, "P0", depth})
                .observeOn(Schedulers.computation())
                .scan(
                        ImmutableTriple.of(
                                new BitfinexOrderbook(new BitfinexOrderbookLevel[0]), (Integer) null, (OrderBook) null
                        ),
                        (immutableTriple, jsonNode) -> {
                            if ("cs".equals(jsonNode.get(1).asText())) {
                                return ImmutableTriple.of(
                                        immutableTriple.getLeft(), jsonNode.get(2).asInt(), immutableTriple.getRight()
                                );
                            } else {
                                final BitfinexOrderbook bitfinexOrderbook = (
                                        jsonNode.get(1).size() == 0 || jsonNode.get(1).get(0).isArray()
                                                ? mapper.treeToValue(jsonNode, BitfinexWebSocketSnapshotOrderbook.class)
                                                : mapper.treeToValue(jsonNode, BitfinexWebSocketUpdateOrderbook.class)
                                )
                                        .toBitfinexOrderBook(immutableTriple.getLeft());
                                return ImmutableTriple.of(
                                        bitfinexOrderbook,
                                        null,
                                        adaptOrderBook(bitfinexOrderbook.toBitfinexDepth(), currencyPair)
                                );
                            }
                        }
                )
                .share();

        final Disposable checksumDisposable = subscribedChannel.filter(triple -> triple.getMiddle() != null)
                .sample(1, TimeUnit.MINUTES)
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
                                + " csData " + csData;
                        log.error(msg);
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

    public Observable<BitfinexWebSocketAuthOrder> getRawAuthenticatedOrders() {
        return service.getAuthenticatedOrders();
    }

    public Observable<BitfinexWebSocketAuthPreTrade> getRawAuthenticatedPreTrades() {
        return service.getAuthenticatedPreTrades();
    }

    public Observable<BitfinexWebSocketAuthTrade> getRawAuthenticatedTrades() {
        return service.getAuthenticatedTrades();
    }

    public Observable<BitfinexWebSocketAuthBalance> getRawAuthenticatedBalances() {
        return service.getAuthenticatedBalances();
    }
}
