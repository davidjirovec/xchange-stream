package info.bitrich.xchangestream.bitfinex.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
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
public class BitfinexWebSocketUpdateOrderbook extends BitfinexWebSocketOrderbookTransaction {
    public BitfinexOrderbookLevel level;

    @Override
    public BitfinexOrderbook updateOrderbook(final BitfinexOrderbook oldOrderbook, final CurrencyPair currencyPair) {
        final Date date = new Date();
        return (
                level.amount.signum() < 0
                        ? oldOrderbook
                            .withAsks(updateSide(oldOrderbook.getAsks(), currencyPair, Order.OrderType.ASK, date))
                        : oldOrderbook
                            .withBids(updateSide(oldOrderbook.getBids(), currencyPair, Order.OrderType.BID, date))
        )
                .withDate(date);
    }

    private BitfinexOrderbookSide updateSide(
            final BitfinexOrderbookSide bitfinexOrderbookSide,
            final CurrencyPair currencyPair,
            final Order.OrderType orderType,
            final Date date
    ) {
        final BigDecimal absPrice = level.price.abs();
        final SortedMap<BigDecimal, LimitOrder> map = level.getCount().signum() == 0
                ? bitfinexOrderbookSide.getSortedMap().remove(absPrice)
                : bitfinexOrderbookSide.getSortedMap()
                    .put(
                            absPrice,
                            BitfinexAdapters.adaptOrder(level.amount.abs(), level.price, currencyPair, orderType, date)
                    );
        return new BitfinexOrderbookSide(map, map.values().asJava());
    }
}
