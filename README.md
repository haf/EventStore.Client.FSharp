# EventStore.Client F# API

This project currently targets [EventStore 3.0](http://geteventstore.com/).

This is a project that aims to bring a smooth F# API to your codes.

Currently, has almost APIs mapped out. Documentation is in the form of XML-docs
that accompanies the assembly.

## API

Usage `open EventStore.Client`.

### Global in `EventStore.Client` namespace

 - EventVersion
 - ExpectedVersionUnion
 - ResolveLinksStrategy
 - Connection - a wrapper interface that this nuget implements on top of
   `EventStore.Client.IEventStoreConnection` which allows for easy mocking and
   stubbing against the more precise F# types.

### ConnBuilder

Wrapper F# API for the connection settings builder in the event store client
API.

### Types (auto opened)

F# types for working with EventStore. Complete with reflection to get around the
private constructors.

### Conn

Module with methods for working with `Connection`.

### Tx

Module with methods for working with `EventStoreTransaction`.

### Events

Module that is a helper module for writing sane data about events to the
streams.

### Aggregate (qualified)

Module that is a helper module for working with transducers in the form of
Aggregates from F#.

### Repo

Module that is a helper module for easily loading and saving event-sourced
aggregates.

## Community

Please see the issue tracker on this github - and use it to ask questions.
Anyone is welcome to answer.

## Roadmap

 - [ ] API-parity with v3.0
 - [ ] Snapshot helpers for Aggregates
 - [ ] Fix depdencies for nuget:
   * FSharp.Core.3
   * Newtonsoft.Json
   * EventStore.Client
   * FSharpX.Core