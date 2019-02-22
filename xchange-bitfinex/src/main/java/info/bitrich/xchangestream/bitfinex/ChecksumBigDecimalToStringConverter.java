package info.bitrich.xchangestream.bitfinex;


import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.math.BigDecimal;
import java.util.List;

class ChecksumBigDecimalToStringConverter {

    private final static ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");

    String convert(final List<BigDecimal> bigDecimals) {
        try {
            return ((String) engine.eval(bigDecimals + ".join(':')"));
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
    }

}
