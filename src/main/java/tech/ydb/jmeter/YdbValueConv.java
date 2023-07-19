package tech.ydb.jmeter;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import tech.ydb.table.values.OptionalType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * Text-to-value YDB data type convertors.
 * @author zinal
 */
public class YdbValueConv {

    public static Value<?> convert(String type, String value) {
        boolean optional = false;
        if (type.endsWith("?")) {
            optional = true;
            type = type.substring(0, type.length()-1);
        }
        Conv conv = HANDLERS.get(type);
        if (conv==null) {
            throw new IllegalArgumentException("Unsupported data type: " + type);
        }
        return conv.convert(value, optional);
    }

    private static RuntimeException makeIllegalEmpty() {
        return new IllegalArgumentException("Empty value for non-optional parameter");
    }
    
    private static final HashMap<String, Conv> HANDLERS = new HashMap<>();
    static {
        reg(new ConvBool());
        reg(new ConvInt32());
        reg(new ConvUint32());
        reg(new ConvInt64());
        reg(new ConvUint64());
        reg(new ConvFloat());
        reg(new ConvDouble());
        reg(new ConvDate());
        reg(new ConvDateTime());
        reg(new ConvTimestamp());
        reg(new ConvText());
        reg(new ConvBytes());
        reg(new ConvBase64());
    }

    private static void reg(Conv conv) {
        HANDLERS.put(conv.name().toLowerCase(), conv);
    }

    private static interface Conv {
        String name();
        Value<?> convert(String value, boolean optional);
    }

    private static abstract class ConvNum implements Conv {
        private final String name;
        private final OptionalType otype;

        protected ConvNum(String name, Type type) {
            this.name = name;
            this.otype = type.makeOptional();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Value<?> convert(String value, boolean optional) {
            if (value!=null) {
                value = value.trim();
            }
            if (value==null || value.length()==0) {
                if (optional)
                    return otype.emptyValue();
                throw makeIllegalEmpty();
            }
            Value<?> v = convertNum(value);
            if (optional)
                v = v.makeOptional();
            return v;
        }
        protected abstract Value<?> convertNum(String value);
    }

    private static class ConvBool extends ConvNum {
        public ConvBool() {
            super("Bool", PrimitiveType.Bool);
        }
        @Override
        public Value<?> convertNum(String value) {
            if (value.equalsIgnoreCase("true")
                    || value.equalsIgnoreCase("yes")
                    || value.equalsIgnoreCase("t")
                    || value.equalsIgnoreCase("1"))
                return PrimitiveValue.newBool(true);
            if (value.equalsIgnoreCase("false")
                    || value.equalsIgnoreCase("no")
                    || value.equalsIgnoreCase("f")
                    || value.equalsIgnoreCase("0"))
                return PrimitiveValue.newBool(true);
            throw new IllegalArgumentException("Illegal value for boolean: " + value);
        }
    }

    private static class ConvInt32 extends ConvNum {
        public ConvInt32() {
            super("Int32", PrimitiveType.Int32);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newInt32(Integer.parseInt(value));
        }
    }

    private static class ConvUint32 extends ConvNum {
        public ConvUint32() {
            super("Uint32", PrimitiveType.Uint32);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newUint32(Long.parseLong(value));
        }
    }

    private static class ConvInt64 extends ConvNum {
        public ConvInt64() {
            super("Int64", PrimitiveType.Int64);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newInt64(Long.parseLong(value));
        }
    }

    private static class ConvUint64 extends ConvNum {
        public ConvUint64() {
            super("Uint64", PrimitiveType.Uint64);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newUint64(Long.parseLong(value));
        }
    }

    private static class ConvFloat extends ConvNum {
        public ConvFloat() {
            super("Float", PrimitiveType.Float);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newFloat(Float.parseFloat(value));
        }
    }

    private static class ConvDouble extends ConvNum {
        public ConvDouble() {
            super("Double", PrimitiveType.Double);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newDouble(Double.parseDouble(value));
        }
    }

    private static class ConvDate extends ConvNum {
        public ConvDate() {
            super("Date", PrimitiveType.Date);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newDate(LocalDate.parse(value));
        }
    }

    private static class ConvDateTime extends ConvNum {
        public ConvDateTime() {
            super("DateTime", PrimitiveType.Datetime);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newDatetime(LocalDateTime.parse(value));
        }
    }

    private static class ConvTimestamp extends ConvNum {
        public ConvTimestamp() {
            super("Timestamp", PrimitiveType.Timestamp);
        }
        @Override
        public Value<?> convertNum(String value) {
            return PrimitiveValue.newTimestamp(Instant.parse(value));
        }
    }

    private static class ConvText implements Conv {
        private final OptionalType otype = PrimitiveType.Text.makeOptional();

        @Override
        public String name() {
            return "Text";
        }

        @Override
        public Value<?> convert(String value, boolean optional) {
            if (value==null) {
                if (optional) {
                    return otype.emptyValue();
                }
                throw makeIllegalEmpty();
            }
            return PrimitiveValue.newText(value);
        }
    }

    private static class ConvBytes implements Conv {
        private final OptionalType otype = PrimitiveType.Bytes.makeOptional();

        @Override
        public String name() {
            return "Bytes";
        }

        @Override
        public Value<?> convert(String value, boolean optional) {
            if (value==null) {
                if (optional) {
                    return otype.emptyValue();
                }
                throw makeIllegalEmpty();
            }
            return PrimitiveValue.newBytes(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class ConvBase64 implements Conv {
        private final OptionalType otype = PrimitiveType.Bytes.makeOptional();

        @Override
        public String name() {
            return "Base64";
        }

        @Override
        public Value<?> convert(String value, boolean optional) {
            if (value==null) {
                if (optional) {
                    return otype.emptyValue();
                }
                throw makeIllegalEmpty();
            }
            return PrimitiveValue.newBytes(java.util.Base64.getUrlDecoder().decode(value));
        }
    }

}