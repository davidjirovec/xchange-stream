package info.bitrich.xchangestream.quione;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.service.pusher.PusherStreamingService;
import io.reactivex.Completable;
import org.knowm.xchange.quoine.QuoineExchange;

public class QuoineStreamingExchange extends QuoineExchange implements StreamingExchange {
    private static final String API_KEY = "LIQUID";
    private static final String HOST = "tap.liquid.com";
    private final PusherStreamingService streamingService;

    private QuoineStreamingMarketDataService streamingMarketDataService;

    public QuoineStreamingExchange() {
        streamingService = new PusherStreamingService(API_KEY, HOST);
    }

    @Override
    protected void initServices() {
        super.initServices();
        streamingMarketDataService = new QuoineStreamingMarketDataService(streamingService);
    }

    @Override
    public Completable connect(ProductSubscription... args) {
        return streamingService.connect();
    }

    @Override
    public Completable disconnect() {
        return streamingService.disconnect();
    }

    @Override
    public StreamingMarketDataService getStreamingMarketDataService() {
        return streamingMarketDataService;
    }

    @Override
    public boolean isAlive() {
        return streamingService.isSocketOpen();
    }

    @Override
    public void useCompressedMessages(boolean compressedMessages) {
        streamingService.useCompressedMessages(compressedMessages);
    }
}
