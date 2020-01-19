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
        this(new BitfinexOrderbookSide(false), new BitfinexOrderbookSide(true), new Date());
    }

    public BitfinexOrderbook(final BitfinexOrderbookSide asks, final BitfinexOrderbookSide bids, final Date date) {
        this.asks = asks;
        this.bids = bids;
        this.date = date;
    }

    public BitfinexOrderbookSide getAsks() {
        return asks;
    }

    public BitfinexOrderbookSide getBids() {
        return bids;
    }

    public Date getDate() {
        return date;
    }

}
