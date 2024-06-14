## Querator
A FIFO queue backed by a Database with only once delivery semantics.

### ZOMG WHY?
Because everyone at one time in their career has implemented a queue using PostgreSQL or MySQL, etc.... 

> From [Stack Overflow](https://stackoverflow.com/questions/423111/whats-the-best-way-of-implementing-a-messaging-queue-table-in-mysql)
>  It's probably the tenth time I'm implementing something like this (A queue using a DB), and I've never been 100% happy about solutions I came up with.
>  The reason I'm using mysql table instead of a "proper" messaging system is primarily because most application already use some relational database for
>  other stuff (which tends to be mysql for most of the stuff I've been doing), while very few applications use a messaging system. 

I feel your pain random enterprise programmer, I feel it in my bones...

See [Querator OSS API Reference](https://thrawn01-llc.stoplight.io/docs/querator-oss/924788fc33955-querator-oss-api) for and idea of what the API looks like.