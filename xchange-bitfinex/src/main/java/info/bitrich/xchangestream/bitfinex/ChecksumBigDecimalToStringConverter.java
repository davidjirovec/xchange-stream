package info.bitrich.xchangestream.bitfinex;


import org.apache.commons.lang3.StringUtils;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

class ChecksumBigDecimalToStringConverter {

    String convert(final List<BigDecimal> bigDecimals) {
        final ScriptEngine engine = new ScriptEngineManager().getEngineByName("javascript");
        ScriptContext context = engine.getContext();
        StringWriter writer = new StringWriter();
        context.setWriter(writer);

        try {
            engine.eval("print([" + bigDecimals.stream().map(BigDecimal::toString).collect(Collectors.joining(",")) + "].join(':'))");
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }

        return StringUtils.deleteWhitespace(writer.toString());
    }

}
