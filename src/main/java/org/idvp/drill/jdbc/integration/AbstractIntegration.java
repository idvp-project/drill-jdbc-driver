package org.idvp.drill.jdbc.integration;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractIntegration implements Integration {

    @Override
    public Object instance(String className) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = this.getClass().getClassLoader();
        }

        try {
            Class<?> $class = Class.forName(className, true, classLoader);
            return createInstance($class);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    protected abstract Object createInstance(Class<?> $class);
}
