package io.quarkiverse.lock.nats.runtime;

import java.nio.charset.StandardCharsets;

/**
 * Maps lock keys to valid NATS KV keys, deterministically and injectively: characters allowed in KV keys pass through,
 * everything else (and {@code =} itself) becomes {@code =XX} per UTF-8 byte. Dots are kept as token separators unless
 * they would produce an empty token (leading, trailing or doubled dots).
 */
public final class KeyCodec {

    public static final int MAX_KEY_LENGTH = 255;

    private KeyCodec() {
    }

    public static String encode(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Lock key must not be empty");
        }
        StringBuilder sb = new StringBuilder(key.length());
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            boolean dotOk = b == '.' && i > 0 && i < bytes.length - 1 && bytes[i - 1] != '.';
            if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '-' || b == '_'
                    || b == '/' || dotOk) {
                sb.append((char) b);
            } else {
                sb.append('=').append(Character.toUpperCase(Character.forDigit(b >> 4, 16)))
                        .append(Character.toUpperCase(Character.forDigit(b & 0xF, 16)));
            }
        }
        if (sb.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("Lock key '" + key + "' is too long for NATS KV: " + sb.length()
                    + " characters encoded, at most " + MAX_KEY_LENGTH);
        }
        return sb.toString();
    }
}
