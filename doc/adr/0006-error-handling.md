# 6. Error Handling

Date: 2024-06-28

## Status

Accepted

## Context

Consistent error message reporting and handling from the server so clients can easily capture the cause in a 
machine and human friendly manner.

## Decision

#### Error Structs
The code will use the same error structs throughout all the abstraction layers. Both internal and public abstraction
layers will use the same error structs. Ideally abstractions should never leak their details across boundaries, however
since the main purpose of this code base is to implement a service accessible via some network protocol, there is 
no need to hide nor complicate error handling in order to preserve abstraction layer separation.

There are essentially 2 classes of errors.
1. Human / Client readable errors that will be returned to the client.
2. Internal Errors which contain details about the internal workings of the service. These errors will be logged
   and a generic `Internal Error` message will be returned to the client.

The `transport` package holds all the Human / Client readable errors. If you wish to return a message to the 
client use one of the errors defined in `transport/errors.go`. All other non `duh.Error` errors will be treated
as internal errors.

Internal errors should include as much detail about the error as possible and will use `github.com/kapetan-io/errors`
to provide context about the error which will be extracted and logged. Internal errors should also include the
name of the function call which caused the error. For example `return fmt.Errorf("during Begin(): %w", err)`

#### Client Readable Message Format
All client errors will be in the format `<const message>;<details of the error>`. This format allows clients to
extract the `const message` by splitting the error message on the `;` character. The `const message` is static,
meaning it remains the same for each error type. This allows the client to compare the `const message` to a list
of known messages and take some predetermined action based on the message. In contrast, the `details of the error`
can and should include details about the specifics of this particular error, which are not easily mapped to a 
list of known errors.

Consider the error `parameter cookie_time is invalid; value 'everyday' is not in the format YYYY-MM-DD HH:MM:DD`.
For this error, the `const message` is `parameter cookie_time is invalid`, which never changes. Thus, the client
can safely match this error against a known list of errors and take appropriate action without user intervention.
The details portion `value 'everyday' is not in the format YYYY-MM-DD HH:MM:DD` includes the value that the client
gave the server, which was invalid (`everyday` is not in an acceptable format). This message will change depending
on the value of `cookie_time` provided by the user.

More examples include:
- `parameter rainbow_dash is invalid; value '0%' is not high enough, value must be greater than 20% awesome`.
- `parameter credit_card is invalid; value '1234-5678-9012-3456' is not a valid credit card number`
- `bounty is of wrong type; the value 'Greedo' is not 'Han Solo', bring me 'Han Solo', and a cookie`

An error message does not need to include the `<details of the error>` or a `;`. It can consist of only the 
`const message`. for example `the queue name cannot be empty`.

#### Parameter names
When referring to parameters or values passed via protobuf/json. Errors should use the JSON name when referring to
a value.  

#### TODO
Once we have a documentation site up, all errors should include a URL to the documentation site where more
details about the error can be found.

## Consequences

It's possible not all messages fit into this format, but I've yet to find one.
