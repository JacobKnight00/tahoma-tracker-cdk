package com.tahomatracker.service.api;

import javax.inject.Singleton;

import dagger.Component;

/**
 * Dagger component for Admin API Lambda dependencies.
 */
@Singleton
@Component(modules = {com.tahomatracker.service.modules.AdminApiModule.class})
public interface AdminApiComponent {
    AdminLabelService adminLabelService();
    AdminApiConfig config();
}
