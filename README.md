# Rust bindings for accessing the Go containers/image stack

This crate contains a Rust API that forks `/usr/bin/skopeo` and
talks to it via a custom API.  You can use it to fetch container
images in a streaming fashion.

At the time of this writing, you will need skopeo 1.6.0 or later.

# Why?

First, assume one is operating on a codebase that isn't Go, but wants
to interact with container images - we can't just include the Go containers/image
library.

The primary intended use case of this is for things like
[ostree-containers](https://github.com/ostreedev/ostree-rs-ext/issues/18)
where we're using container images to encapsulate host operating system
updates, but we don't want to involve the [containers/image](github.com/containers/image/)
storage layer.

What we *do* want from the containers/image library is support for things like
signatures and offline mirroring.  More on this below.

Forgetting things like ostree exist for a second - imagine that you wanted to 
encapsulate a set of Debian/RPM/etc packages inside
a container image to ship for package-based operating systems.  You could use this to stream
out the layer containing those packages and extract them directly, rather than serializing
everything to disk in the containers/storage disk location, only to copy it out again and delete the first.

Another theoretical use case could be something like [krustlet](https://github.com/deislabs/krustlet),
which fetches WebAssembly blobs inside containers.  Here again, we don't want to involve
containers/storage.

# Desired containers/image features

There are e.g. Rust libraries like [dkregistry-rs](https://github.com/camallo/dkregistry-rs) and
[oci-distribution](https://crates.io/crates/oci-distribution) and similar for other languages.

However, the containers/image Go library has a lot of additional infrastructure
that will impose a maintenance burden to replicate:

 - Signatures (`man containers-auth.json`)
 - Mirroring/renaming (`man containers-registries.conf`)
 - Support for `~/.docker/config.json` for authentication as well as `/run`

# Status

API is subject to change.
