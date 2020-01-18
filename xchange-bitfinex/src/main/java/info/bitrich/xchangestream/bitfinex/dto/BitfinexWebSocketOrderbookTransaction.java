package info.bitrich.xchangestream.bitfinex.dto;

import org.knowm.xchange.currency.CurrencyPair;

/**
 * Created by Lukas Zaoralek on 8.11.17.
 */
public abstract class BitfinexWebSocketOrderbookTransaction {
    public String channelId;

    public BitfinexWebSocketOrderbookTransaction() {
    }

    public BitfinexWebSocketOrderbookTransaction(String channelId) {
        this.channelId = channelId;
    }

    public String getChannelId() {
        return channelId;
    }

    public abstract BitfinexOrderbook updateOrderbook(BitfinexOrderbook oldOrderbook, CurrencyPair currencyPair);
}
