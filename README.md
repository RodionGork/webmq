# WebMQ

It is an exercise: the goal is to create message broker with web-interface, allowing to put and get
messages with simple HTTP-requests:

    PUT localhost:8000/topic?v=message

    GET localhost:8000/topic

We want to wait with some timeout if there is no message currently in the topic requested with GET.
If the message comes, it should be consumed by those of waiting clients who requested it first.

