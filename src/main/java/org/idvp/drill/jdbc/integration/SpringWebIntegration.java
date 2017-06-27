package org.idvp.drill.jdbc.integration;

import org.springframework.web.context.ContextLoader;

/**
 * @author Oleg Zinoviev
 * @since 27.06.2017.
 */
@SuppressWarnings("unused")
public class SpringWebIntegration extends AbstractIntegration {
    @Override
    protected Object createInstance(Class<?> $class) {
        return ContextLoader.getCurrentWebApplicationContext().getBean($class);
    }
}
