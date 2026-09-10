# Support and release status

Justflow 0.1 is beta software. APIs and schemas are versioned, but may change before 1.0.
See [limitations](limitations.md) for supported Python versions and known limits. The changelog
records behavior and compatibility changes for each release.

## Getting help

Open a [GitHub issue](https://github.com/nickyua/justflow/issues) for questions or bug reports.
For a bug, include your Justflow and Python versions, how you run the application, the error code,
and the smallest example that reproduces it. Remove credentials, customer data, and private
addresses from configuration and logs. Full Temporal histories can contain application data.

Support is provided by the community on a best-effort basis. There is no guaranteed response time
or hosted service included with the library.

## Compatibility reports

For replay or upgrade problems, include the old and new worker build IDs, definition digest,
schema/API version, and provider versions. A replay example using synthetic data helps reproduce
the problem. Keep the original history and build artifacts privately until the issue is resolved.

## Security

Follow the repository's [security policy](https://github.com/nickyua/justflow/blob/main/SECURITY.md)
to report suspected vulnerabilities privately. Do not post vulnerability details, live credentials,
or customer data in a public issue.

## Release artifacts

The core library and optional admin panel are published as Python wheels and source distributions.
The core package includes the versioned authoring schemas and OpenAPI document.

Container images are published separately when the image publisher is enabled. Those releases
include dependency inventories (SBOMs), signatures, and build attestations. Before deploying an
image, verify its digest and build information, and use the digest to select the exact image.
