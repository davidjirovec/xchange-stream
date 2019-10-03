package info.bitrich.xchangestream.quione;

import com.fasterxml.jackson.core.type.TypeReference;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import info.bitrich.xchangestream.service.pusher.PusherStreamingService;
import io.reactivex.Observable;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.quoine.QuoineAdapters;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class QuoineStreamingMarketDataService implements StreamingMarketDataService {
    private final PusherStreamingService service;

    QuoineStreamingMarketDataService(PusherStreamingService service) {
        this.service = service;
    }

    @Override
    public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
        return Observable.combineLatest(
                getOrderBookSide(currencyPair, "sell", Order.OrderType.ASK),
                getOrderBookSide(currencyPair, "buy", Order.OrderType.BID),
                (asks, bids) -> new OrderBook(null, asks, bids)
        );
    }

    private Observable<List<LimitOrder>> getOrderBookSide(
            final CurrencyPair currencyPair, final String quoineOrderType, final Order.OrderType orderType
    ) {
        return service
                .subscribeChannel(getChannelName(currencyPair, quoineOrderType), "updated")
                .map(string ->
                        StreamingObjectMapperHelper.getObjectMapper()
                                .<List<List<BigDecimal>>>readValue(
                                        string, new TypeReference<List<List<BigDecimal>>>() {}
                                )
                                .stream()
                                .map(bigDecimals -> getLimitOrder(currencyPair, orderType, bigDecimals))
                                .collect(Collectors.toList())
                );
    }

    private String getChannelName(final CurrencyPair currencyPair, final String quoineOrderType) {
        return String.format(
                "price_ladders_cash_%s_%s", QuoineAdapters.toPairString(currencyPair).toLowerCase(), quoineOrderType
        );
    }

    private LimitOrder getLimitOrder(
            final CurrencyPair currencyPair, final Order.OrderType orderType, final List<BigDecimal> bigDecimals
    ) {
        return new LimitOrder(orderType, bigDecimals.get(1), currencyPair, "", null, bigDecimals.get(0));
    }

    @Override
    public Observable<Ticker> getTicker(final CurrencyPair currencyPair, final Object... args) {
        throw new NotYetImplementedForExchangeException();
    }

    @Override
    public Observable<Trade> getTrades(final CurrencyPair currencyPair, final Object... args) {
        throw new NotYetImplementedForExchangeException();
    }

}
