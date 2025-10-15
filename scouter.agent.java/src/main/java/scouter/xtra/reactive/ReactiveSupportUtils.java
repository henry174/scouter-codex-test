package scouter.xtra.reactive;

import reactor.core.publisher.Mono;
import reactor.util.context.Context;
import scouter.agent.Logger;
import scouter.agent.trace.TraceContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * @author Gun Lee (gunlee01@gmail.com) on 2/21/24
 */
public class ReactiveSupportUtils {

    private static final Class<?> CONTEXT_VIEW_CLASS = loadContextViewClass();
    private static final Method CONTEXT_VIEW_GET_OR_DEFAULT = loadContextViewMethod("getOrDefault", Object.class, Object.class);
    private static final Method CONTEXT_VIEW_FOR_EACH = loadContextViewMethod("forEach", BiConsumer.class);

    private static volatile boolean contextCopyWarningLogged = false;

    public static boolean isSupportReactor34() {
        try {
            Class<Mono> monoClass = Mono.class;
            Class<?>[] parameterTypes = new Class<?>[]{Function.class};
            monoClass.getMethod("contextWrite", parameterTypes);

            return true;
        } catch (NoSuchMethodException e) {
            Logger.println("R301", e.getMessage());
            return false;
        } catch (Exception e) {
            Logger.println("R302", e.getMessage(), e);
            return false;
        }
    }

    public static Object putTraceContext(Object contextLike, TraceContext traceContext) {
        try {
            Context context = toWritableContext(contextLike);
            return context.put(TraceContext.class, traceContext);
        } catch (Throwable t) {
            Logger.println("R304", t.getMessage(), t);
            return contextLike;
        }
    }

    public static TraceContext getTraceContext(Object contextLike) {
        if (contextLike instanceof Context) {
            return ((Context) contextLike).getOrDefault(TraceContext.class, null);
        }
        if (CONTEXT_VIEW_CLASS != null && CONTEXT_VIEW_CLASS.isInstance(contextLike) && CONTEXT_VIEW_GET_OR_DEFAULT != null) {
            try {
                return (TraceContext) CONTEXT_VIEW_GET_OR_DEFAULT.invoke(contextLike, TraceContext.class, null);
            } catch (IllegalAccessException | InvocationTargetException e) {
                Logger.println("R305", e.getMessage(), e);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getOrDefault(Object contextLike, Class<T> key, T defaultValue) {
        if (contextLike instanceof Context) {
            return ((Context) contextLike).getOrDefault(key, defaultValue);
        }
        if (CONTEXT_VIEW_CLASS != null && CONTEXT_VIEW_CLASS.isInstance(contextLike) && CONTEXT_VIEW_GET_OR_DEFAULT != null) {
            try {
                return (T) CONTEXT_VIEW_GET_OR_DEFAULT.invoke(contextLike, key, defaultValue);
            } catch (IllegalAccessException | InvocationTargetException e) {
                Logger.println("R306", e.getMessage(), e);
            }
        }
        return defaultValue;
    }

    public static Context toWritableContext(Object contextLike) {
        if (contextLike instanceof Context) {
            return (Context) contextLike;
        }
        if (CONTEXT_VIEW_CLASS != null && CONTEXT_VIEW_CLASS.isInstance(contextLike)) {
            Context empty = Context.empty();
            if (CONTEXT_VIEW_FOR_EACH == null) {
                logContextCopyWarning();
                return empty;
            }
            final AtomicReference<Context> ref = new AtomicReference<>(empty);
            try {
                CONTEXT_VIEW_FOR_EACH.invoke(contextLike, new BiConsumer<Object, Object>() {
                    @Override
                    public void accept(Object key, Object value) {
                        ref.set(ref.get().put(key, value));
                    }
                });
            } catch (IllegalAccessException | InvocationTargetException e) {
                Logger.println("R307", e.getMessage(), e);
                return empty;
            }
            return ref.get();
        }
        return Context.empty();
    }

    private static Class<?> loadContextViewClass() {
        try {
            return Class.forName("reactor.util.context.ContextView");
        } catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    private static Method loadContextViewMethod(String name, Class<?>... paramTypes) {
        if (CONTEXT_VIEW_CLASS == null) {
            return null;
        }
        try {
            return CONTEXT_VIEW_CLASS.getMethod(name, paramTypes);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static void logContextCopyWarning() {
        if (contextCopyWarningLogged) {
            return;
        }
        contextCopyWarningLogged = true;
        Logger.println("R308", "ContextView#forEach not accessible; reactor context copying may be incomplete.");
    }
}
