Thanks for your interest in contributing to Querator! Please take a moment to review this document
**before submitting a pull request**.

# Pull requests

**Please ask first before starting work on any significant new features.**

We love to code, and as such, we tend to code first and talk later. (wut? I haz to talk with people?) However, it's
never a fun experience to have your pull request declined after investing joy, time and effort into a new feature.

To avoid this from happening to you, we request that contributors create a GitHub issue and tag a maintainer, so we
can first discuss any new feature ideas. Your ideas and suggestions are welcome!

Bug reports are always welcome, but if the bug report requires a large refactor to fix, you might want to create
and issue and discuss.

## Fork and create a branch
If this is something you think you can fix, then fork https://github.com/kapetan-io/querator and create
a branch with a descriptive name.

## Style Guide
See [Naming Guide](doc/NAMING.md)

## Functional Tests
Functional tests should always make up 90% of all testing done on Querator. If there is a bit of code that
should be tested, and can't be deleted, you may create a unit test to exercise that bit of code. Unit tests
are a last resort, because by their very nature of exercising the implementation instead of the effect of the
interface, they create a maintenance burden. TODO: Link to an article on this topic.

#### Running an individual functional test
Functional tests are organized in a nested fashion which allows a specific test to be run in isolation yet can depend
on previous tests. The nature of these nested tests are such that any prerequisite tests are run before the individual
test we are targeting. When adding new tests we should ensure that each test can run individually such that we nest 
the dependent test inside the prerequisite test.

The tests are organized such that tests that CAN run in parallel do, and tests which cannot will not.

You can select a failing test by copying the full path of the test from running `go test ./... -v` output.

For example
```bash
=== RUN   TestQueue/InMemory/Errors/QueueLease/RequestTimeoutInvalid
--- PASS: TestQueue/InMemory/Errors/QueueLease/RequestTimeoutInvalid (0.00s)
=== RUN   TestQueue/InMemory/Errors/QueueLease/MinimumRequestTimeoutIsAllowed
--- PASS: TestQueue/InMemory/Errors/QueueLease/MinimumRequestTimeoutIsAllowed (0.00s)
=== RUN   TestQueue/InMemory/Errors/QueueLease/RequestTimeoutIsTooShort
--- PASS: TestQueue/InMemory/Errors/QueueLease/RequestTimeoutIsTooShort (0.00s)
=== RUN   TestQueue/InMemory/Errors/QueueLease/DuplicateClientId
    queue_test.go:1018: 
        	Error Trace:	/Users/thrawn/Development/querator/queue_test.go:1018
        	Error:      	Received unexpected error:
        	            	context deadline exceeded
        	Test:       	TestQueue/InMemory/Errors/QueueLease/DuplicateClientId
--- FAIL: TestQueue/InMemory/Errors/QueueLease/DuplicateClientId (5.02s)
```

You can run the failing test `DuplicateClientId` in isolation by running

```bash
$ go test ./... -run "^TestQueue/InMemory/Errors/QueueLease/DuplicateClientId$"
```

You can also run an entire sub suite of tests by using regex wild cards. For example, you can run only tests for 
the `InMemory` storage system by using the following.

```bash
$ go test ./... -v -run "^TestQueue/InMemory.*$"
```

Test names must always use CamelCase and not snake_case nor contain spaces.

## Run all the CI checks
Before creating a pull request, you can run all the CI checks with all the bells and whistles by running.
```bash
$ git add .
$ make ci
```
This will run the linter, go mod tidy check and the full test suite.

### Commits & Messages
We don't have a specific structure for commit messages, but please be as descriptive as possible and avoid
lots of tiny commits. If this is your style, please squash before making a PR, or we may squash on merge for you.

### Create a Pull Request
At this point, your changes look good, and tests are passing, you are ready to create a pull request. Please
explain WHY the PR exists in the description, and link to any related PR's or Issues.
