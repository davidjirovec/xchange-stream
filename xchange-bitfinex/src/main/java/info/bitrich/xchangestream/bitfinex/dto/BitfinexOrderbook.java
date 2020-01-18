package info.bitrich.xchangestream.bitfinex.dto;

import java.util.Date;

/**
 * Created by Lukas Zaoralek on 8.11.17.
 */
public class BitfinexOrderbook {

    private final BitfinexOrderbookSide asks;
    private final BitfinexOrderbookSide bids;
    private final Date date;

    public BitfinexOrderbook() {
        this(new BitfinexOrderbookSide(), new BitfinexOrderbookSide(), new Date());
    }

    public BitfinexOrderbook(final BitfinexOrderbookSide asks, final BitfinexOrderbookSide bids, final Date date) {
        this.asks = asks;
        this.bids = bids;
        this.date = date;
    }

    public BitfinexOrderbookSide getAsks() {
        return asks;
    }

    public BitfinexOrderbook withAsks(final BitfinexOrderbookSide asks) {
        return new BitfinexOrderbook(asks, bids, date);
    }

    public BitfinexOrderbookSide getBids() {
        return bids;
    }

    public BitfinexOrderbook withBids(final BitfinexOrderbookSide bids) {
        return new BitfinexOrderbook(asks, bids, date);
    }

    public Date getDate() {
        return date;
    }

    public BitfinexOrderbook withDate(final Date date) {
        return new BitfinexOrderbook(asks, bids, date);
    }
}
