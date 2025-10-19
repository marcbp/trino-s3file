package com.example.trino.s3file;

import java.util.Arrays;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Lightweight formatter around {@link java.util.logging.Logger} using "{}" style placeholders.
 */
public final class S3FileLogger {
    private final Logger delegate;

    private S3FileLogger(Class<?> target) {
        this.delegate = Logger.getLogger(target.getName());
    }

    public static S3FileLogger get(Class<?> target) {
        return new S3FileLogger(target);
    }

    public void info(String message, Object... args) {
        log(Level.INFO, message, args);
    }

    public void debug(String message, Object... args) {
        log(Level.FINE, message, args);
    }

    public void trace(String message, Object... args) {
        log(Level.FINER, message, args);
    }

    public void error(String message, Object... args) {
        log(Level.SEVERE, message, args);
    }

    private void log(Level level, String message, Object... args) {
        Throwable throwable = extractThrowable(args);
        Object[] formatArgs = stripThrowable(args);
        String formatted = format(message, formatArgs);
        if (throwable != null) {
            delegate.log(level, formatted, throwable);
        }
        else {
            delegate.log(level, formatted);
        }
    }

    private static Throwable extractThrowable(Object[] args) {
        if (args != null && args.length > 0) {
            Object last = args[args.length - 1];
            if (last instanceof Throwable) {
                return (Throwable) last;
            }
        }
        return null;
    }

    private static Object[] stripThrowable(Object[] args) {
        if (args == null || args.length == 0) {
            return args;
        }
        if (args[args.length - 1] instanceof Throwable) {
            return Arrays.copyOf(args, args.length - 1);
        }
        return args;
    }

    private static String format(String message, Object... args) {
        if (args == null || args.length == 0) {
            return message;
        }
        String fmt = message.replace("{}", "%s");
        return String.format(Locale.ROOT, fmt, args);
    }
}
