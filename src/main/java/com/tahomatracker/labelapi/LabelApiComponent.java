package com.tahomatracker.labelapi;

import javax.inject.Singleton;

import com.tahomatracker.di.AwsClientsModule;
import com.tahomatracker.di.LabelApiModule;

import dagger.Component;

/**
 * Dagger component for the Label API Lambda.
 *
 * Combines the shared AWS clients module with Label API specific bindings.
 * The component is singleton-scoped to ensure clients are reused across invocations.
 */
@Singleton
@Component(modules = {AwsClientsModule.class, LabelApiModule.class})
public interface LabelApiComponent {

    /**
     * Provides the LabelSubmissionService with all dependencies injected.
     */
    LabelSubmissionService labelSubmissionService();

    /**
     * Provides the config.
     */
    LabelApiConfig config();

    /**
     * Builder for the component, allowing module instances to be provided.
     */
    @Component.Builder
    interface Builder {
        Builder labelApiModule(LabelApiModule module);
        LabelApiComponent build();
    }
}
