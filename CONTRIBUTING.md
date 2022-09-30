# Contributing to Plumber

First off, thanks for taking the time to contribute! ❤️

All types of contributions are encouraged and valued. 

> And if you like the project, but just don't have time to contribute, that's fine. There are other easy ways to support the project and show your appreciation, which we would also be very happy about:
> - Star the project
> - Tweet about it
> - Refer this project in your project's README
> - Mention the project at local meetups and tell your friends/colleagues

## Code of Conduct

We try to adhere to the [Go Community Code of Conduct](https://go.dev/conduct). Please read it and make sure you understand it.
We want to make the community as welcoming and helpful as possible. 

## Commit Messages

When possible please try to be descriptive in your commit messages. It helps us understand what you are trying to accomplish and why.

## Filing Bug Reports

When you notice something broken, please open an issue using the Bug Report template. We will try to get back to you as soon as possible.

## Feature Requests

When you have a feature request, please open an issue and describe the feature you would like to see using the Feature Request template.

## Writing Tests

`plumber` uses [gingko](https://onsi.github.io/ginkgo/) and [gomega](https://onsi.github.io/gomega/) for testing. We'd prefer tests to be written using these frameworks and suggest
taking a look at existing tests or the [gingko](https://onsi.github.io/ginkgo/#writing-specs) documentation for more information. We're happy to help if you have any questions.

## Vendoring

We still use `/vendor`! It may have been a bit since you've used it, but if you happen to need to update a dependency, make sure to run `go mod vendor` after updating your code to ensure that any dependencies are updated in the vendor cache as well.
