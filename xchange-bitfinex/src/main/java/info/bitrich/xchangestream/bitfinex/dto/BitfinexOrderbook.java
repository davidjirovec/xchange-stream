package info.bitrich.xchangestream.bitfinex.dto;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.function.Function;

import static java.math.BigDecimal.ZERO;

/**
 * Created by Lukas Zaoralek on 8.11.17.
 */
public class BitfinexOrderbook {
    private Map<BigDecimal, BitfinexOrderbookLevel> asks;
    private Map<BigDecimal, BitfinexOrderbookLevel> bids;

    public BitfinexOrderbook(BitfinexOrderbookLevel[] levels) {
        createFromLevels(levels);
    }

    public BitfinexOrderbook(final Map<BigDecimal, BitfinexOrderbookLevel> asks, final Map<BigDecimal, BitfinexOrderbookLevel> bids) {
        this.asks = asks;
        this.bids = bids;
    }

    private void createFromLevels(BitfinexOrderbookLevel[] levels) {
        final Tuple2<List<BitfinexOrderbookLevel>, List<BitfinexOrderbookLevel>> partition = List.of(levels)
                .filter(level -> level.getCount().compareTo(ZERO) != 0)
                .partition(level -> level.getAmount().compareTo(ZERO) > 0);

        bids = partition._1.toSortedMap(
                Collections.reverseOrder(), BitfinexOrderbookLevel::getPrice, Function.identity()
        );
        asks = partition._2.toSortedMap(
                BitfinexOrderbookLevel::getPrice,
                level -> new BitfinexOrderbookLevel(level.getPrice(), level.getCount(), level.getAmount().abs())
        );
    }

    public synchronized BitfinexOrderbook withLevel(BitfinexOrderbookLevel level) {
        Map<BigDecimal, BitfinexOrderbookLevel> side;

        // Determine side and normalize negative ask amount values
        BitfinexOrderbookLevel bidAskLevel = level;
        final boolean isNewAsk = level.getAmount().compareTo(ZERO) < 0;
        if(isNewAsk) {
            side = asks;
            bidAskLevel = new BitfinexOrderbookLevel(
                    level.getPrice(),
                    level.getCount(),
                    level.getAmount().abs()
            );
        } else {
            side = bids;
        }

        boolean shouldDelete = bidAskLevel.getCount().compareTo(ZERO) == 0;

        side = side.remove(bidAskLevel.getPrice());
        if (!shouldDelete) {
            side = side.put(bidAskLevel.getPrice(), bidAskLevel);
        }

        return isNewAsk ? new BitfinexOrderbook(side, bids) : new BitfinexOrderbook(asks, side);
    }
}
