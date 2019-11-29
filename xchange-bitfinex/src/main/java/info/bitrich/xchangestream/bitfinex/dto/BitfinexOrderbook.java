package info.bitrich.xchangestream.bitfinex.dto;

import org.knowm.xchange.bitfinex.v1.dto.marketdata.BitfinexDepth;
import org.knowm.xchange.bitfinex.v1.dto.marketdata.BitfinexLevel;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
        this.asks = new HashMap<>(levels.length / 2);
        this.bids = new HashMap<>(levels.length / 2);

        for (BitfinexOrderbookLevel level : levels) {

            if(level.getCount().compareTo(ZERO) == 0)
                continue;

            if (level.getAmount().compareTo(ZERO) > 0)
                bids.put(level.getPrice(), level);
            else
                asks.put(level.getPrice(),
                        new BitfinexOrderbookLevel(
                        level.getPrice(),
                        level.getCount(),
                        level.getAmount().abs()
                ));
        }
    }

    public synchronized BitfinexDepth toBitfinexDepth() {
        SortedMap<BigDecimal, BitfinexOrderbookLevel> bitfinexLevelAsks = new TreeMap<>();
        SortedMap<BigDecimal, BitfinexOrderbookLevel> bitfinexLevelBids = new TreeMap<>(java.util.Collections.reverseOrder());

        for (Map.Entry<BigDecimal, BitfinexOrderbookLevel> level : asks.entrySet()) {
            bitfinexLevelAsks.put(level.getValue().getPrice(), level.getValue());
        }

        for (Map.Entry<BigDecimal, BitfinexOrderbookLevel> level : bids.entrySet()) {
            bitfinexLevelBids.put(level.getValue().getPrice(), level.getValue());
        }

        List<BitfinexLevel> askLevels = new ArrayList<>(asks.size());
        List<BitfinexLevel> bidLevels = new ArrayList<>(bids.size());
        for (Map.Entry<BigDecimal, BitfinexOrderbookLevel> level : bitfinexLevelAsks.entrySet()) {
            askLevels.add(level.getValue().toBitfinexLevel());
        }
        for (Map.Entry<BigDecimal, BitfinexOrderbookLevel> level : bitfinexLevelBids.entrySet()) {
            bidLevels.add(level.getValue().toBitfinexLevel());
        }

        return new BitfinexDepth(askLevels.toArray(new BitfinexLevel[askLevels.size()]),
                bidLevels.toArray(new BitfinexLevel[bidLevels.size()]));
    }

    public synchronized BitfinexOrderbook withLevel(BitfinexOrderbookLevel level) {
        final HashMap<BigDecimal, BitfinexOrderbookLevel> asksCopy = new HashMap<>(asks);
        final HashMap<BigDecimal, BitfinexOrderbookLevel> bidsCopy = new HashMap<>(bids);

        Map<BigDecimal, BitfinexOrderbookLevel> side;

        // Determine side and normalize negative ask amount values
        BitfinexOrderbookLevel bidAskLevel = level;
        if(level.getAmount().compareTo(ZERO) < 0) {
            side = asksCopy;
            bidAskLevel = new BitfinexOrderbookLevel(
                    level.getPrice(),
                    level.getCount(),
                    level.getAmount().abs()
            );
        } else {
            side = bidsCopy;
        }

        boolean shouldDelete = bidAskLevel.getCount().compareTo(ZERO) == 0;

        side.remove(bidAskLevel.getPrice());
        if (!shouldDelete) {
            side.put(bidAskLevel.getPrice(), bidAskLevel);
        }

        return new BitfinexOrderbook(asksCopy, bidsCopy);
    }
}
