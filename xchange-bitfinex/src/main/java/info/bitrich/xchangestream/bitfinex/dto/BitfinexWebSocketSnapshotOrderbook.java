package info.bitrich.xchangestream.bitfinex.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.SortedMap;
import org.knowm.xchange.bitfinex.service.BitfinexAdapters;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.LimitOrder;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Created by Lukas Zaoralek on 8.11.17.
 */
@JsonFormat(shape = JsonFormat.Shape.ARRAY)
public class BitfinexWebSocketSnapshotOrderbook extends BitfinexWebSocketOrderbookTransaction {
    public java.util.List<BitfinexOrderbookLevel> levels;

    @Override
    public BitfinexOrderbook updateOrderbook(final BitfinexOrderbook oldOrderbook, final CurrencyPair currencyPair) {
        final Date date = new Date();
        final Tuple2<SortedMap<BigDecimal, LimitOrder>, SortedMap<BigDecimal, LimitOrder>> maps = List.ofAll(levels)
                .filter(level -> level.amount.signum() != 0)
                .partition(level -> level.amount.signum() < 0)
                .map(
                        list -> toMap(list, currencyPair, Order.OrderType.ASK, date),
                        list -> toMap(list, currencyPair, Order.OrderType.BID, date)
                );
        return new BitfinexOrderbook(
                new BitfinexOrderbookSide(maps._1, maps._1.values().asJava()),
                new BitfinexOrderbookSide(maps._2, maps._2.values().asJava()),
                date
        );
    }

    private SortedMap<BigDecimal, LimitOrder> toMap(
            final io.vavr.collection.List<BitfinexOrderbookLevel> list,
            final CurrencyPair currencyPair,
            final Order.OrderType orderType,
            final Date date
    ) {
        return list.toSortedMap(
                level -> level.price,
                level -> BitfinexAdapters.adaptOrder(level.amount.abs(), level.price, currencyPair, orderType, date)
        );
    }
}
