## Zocalo site configuration for `workflows`

Workflows includes plugins to support [Zocalo site configuration](https://zocalo.readthedocs.io/en/latest/siteconfig.html).

### Stomp (aka. `StompTransport`)

The minimal configuration block to initialise the `StompTransport` class with defaults is:

```yaml
my-stomp-config-block:
    plugin: stomp         # required field to select the stomp configuration plugin
    host: hostname        # IP address, hostname, or FQDN
    port: 61613
    username: username
    password: password1
    prefix: zocalo        # also known as: namespace
```

You then need to load this plugin configuration in an environment,
[as described in the Zocalo documentation](https://zocalo.readthedocs.io/en/latest/siteconfig.html#environment-definitions).
