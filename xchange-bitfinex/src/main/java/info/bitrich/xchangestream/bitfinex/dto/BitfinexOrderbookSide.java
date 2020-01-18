package info.bitrich.xchangestream.bitfinex.dto;

import io.vavr.collection.SortedMap;
import org.knowm.xchange.dto.trade.LimitOrder;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static io.vavr.API.SortedMap;

public class BitfinexOrderbookSide {

    private final SortedMap<BigDecimal, LimitOrder> sortedMap;
    private final List<LimitOrder> list;

    public BitfinexOrderbookSide() {
        this(SortedMap(), Collections.emptyList());
    }

    public BitfinexOrderbookSide(final SortedMap<BigDecimal, LimitOrder> sortedMap, final List<LimitOrder> list) {
        this.sortedMap = sortedMap;
        this.list = list;
    }

    public SortedMap<BigDecimal, LimitOrder> getSortedMap() {
        return sortedMap;
    }

    public List<LimitOrder> getList() {
        return list;
    }
}
