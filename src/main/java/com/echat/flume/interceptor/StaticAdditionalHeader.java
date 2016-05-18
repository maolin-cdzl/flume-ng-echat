package com.echat.flume.interceptor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.echat.flume.interceptor.StaticAdditionalHeader.Constants.*;

/**
 * ...
 *
 * @author daan.debie
 */
public class StaticAdditionalHeader implements Interceptor {

    private final String key;
    private final String value;

    /**
     * Only {@link StaticAdditionalHeader.Builder} can build me
     */
    private StaticAdditionalHeader(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void initialize() {
        // no-op
    }

    /**
     * Modifies events in-place.
     */
    @Override
    public Event intercept(Event event) {
        event.getHeaders().put(key,value);
        return event;
    }

    /**
     * Delegates to {@link #intercept(Event)} in a loop.
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
        // no-op
    }

    /**
     * Builder which builds new instance of the StaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {
		static private Logger logger = LoggerFactory.getLogger(Builder.class);
        private String key = null;
		private String value = null;

        @Override
        public void configure(Context context) {
            String line = context.getString(KEYVAL,KEYVAL_DEFAULT);

			if( line.isEmpty() ) {
				throw new IllegalArgumentException("Must config Key/Value");
			}
			String[] keyValues = line.split("/");
			if( keyValues.length != 2 ) {
				throw new IllegalArgumentException("Only support 1 Key/Value pair");
			}
			key = keyValues[0];
			value = keyValues[1];
        }

        @Override
        public Interceptor build() {
            logger.info(String.format("Creating StaticAdditionalHeader: key=%s,value=%s",key, value));
            return new StaticAdditionalHeader(key, value);
        }


    }

    public static class Constants {
        public static final String KEYVAL = "keyval";
        public static final String KEYVAL_DEFAULT = "";
    }

}

