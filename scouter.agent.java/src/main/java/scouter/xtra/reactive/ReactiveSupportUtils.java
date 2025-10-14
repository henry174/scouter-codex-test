package scouter.xtra.reactive;

import reactor.core.publisher.Mono;
import scouter.agent.Logger;

import java.util.function.Function;

/**
 * @author Gun Lee (gunlee01@gmail.com) on 2/21/24
 */
public class ReactiveSupportUtils {

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
}
