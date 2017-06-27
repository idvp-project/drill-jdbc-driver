package org.idvp.drill.jdbc.integration;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
@SuppressWarnings("unused")
public class DefaultIntegration extends AbstractIntegration {
    @Override
    protected Object createInstance(Class<?> $class) {
        try {
            return $class.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}
