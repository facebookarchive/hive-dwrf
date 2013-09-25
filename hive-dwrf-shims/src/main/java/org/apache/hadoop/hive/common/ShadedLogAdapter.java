package org.apache.hadoop.hive.common;

import org.apache.commons.logging.Log;

public class ShadedLogAdapter
    implements com.facebook.presto.hive.shaded.org.apache.commons.logging.Log
{
    private final Log log;

    public static com.facebook.presto.hive.shaded.org.apache.commons.logging.Log convert(Log log)
    {
        return new ShadedLogAdapter(log);
    }

    public ShadedLogAdapter(Log log)
    {
        this.log = log;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return log.isDebugEnabled();
    }

    @Override
    public boolean isErrorEnabled()
    {
        return log.isErrorEnabled();
    }

    @Override
    public boolean isFatalEnabled()
    {
        return log.isFatalEnabled();
    }

    @Override
    public boolean isInfoEnabled()
    {
        return log.isInfoEnabled();
    }

    @Override
    public boolean isTraceEnabled()
    {
        return log.isTraceEnabled();
    }

    @Override
    public boolean isWarnEnabled()
    {
        return log.isWarnEnabled();
    }

    @Override
    public void trace(Object o)
    {
        log.trace(o);
    }

    @Override
    public void trace(Object o, Throwable throwable)
    {
        log.trace(o, throwable);
    }

    @Override
    public void debug(Object o)
    {
        log.debug(o);
    }

    @Override
    public void debug(Object o, Throwable throwable)
    {
        log.debug(o, throwable);
    }

    @Override
    public void info(Object o)
    {
        log.info(o);
    }

    @Override
    public void info(Object o, Throwable throwable)
    {
        log.info(o, throwable);
    }

    @Override
    public void warn(Object o)
    {
        log.warn(o);
    }

    @Override
    public void warn(Object o, Throwable throwable)
    {
        log.warn(o, throwable);
    }

    @Override
    public void error(Object o)
    {
        log.error(o);
    }

    @Override
    public void error(Object o, Throwable throwable)
    {
        log.error(o, throwable);
    }

    @Override
    public void fatal(Object o)
    {
        log.fatal(o);
    }

    @Override
    public void fatal(Object o, Throwable throwable)
    {
        log.fatal(o, throwable);
    }
}
