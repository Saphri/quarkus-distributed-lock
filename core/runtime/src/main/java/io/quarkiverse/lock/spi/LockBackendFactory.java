package io.quarkiverse.lock.spi;

/**
 * Creates the backend of a given {@link #type()} for a registry. Provider extensions register an implementation as an
 * {@code @ApplicationScoped} bean and announce the type at build time with a {@code LockBackendTypeBuildItem}.
 */
public interface LockBackendFactory {

    /** The value of {@code quarkus.lock[."registry"].type} this factory serves, for example {@code nats}. */
    String type();

    /**
     * Creates the backend for {@code registryName} ({@link io.quarkiverse.lock.LockRegistryName#DEFAULT} for the default
     * registry). Called once per registry, at startup.
     */
    LockBackend create(String registryName);
}
