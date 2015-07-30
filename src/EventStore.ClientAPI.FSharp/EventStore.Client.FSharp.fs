namespace EventStore.ClientAPI

module AsyncHelpers =

  open System
  open System.Threading.Tasks

  type Microsoft.FSharp.Control.Async with
    static member Raise (e : #exn) =
      Async.FromContinuations(fun (_,econt,_) -> econt e)

  let awaitTask (t : System.Threading.Tasks.Task) =
    let flattenExns (e : AggregateException) = e.Flatten().InnerExceptions |> Seq.nth 0
    let rewrapAsyncExn (it : Async<unit>) =
      async { try do! it with :? AggregateException as ae -> do! (Async.Raise <| flattenExns ae) }
    let tcs = new TaskCompletionSource<unit>(TaskCreationOptions.None)
    t.ContinueWith((fun t' ->
      if t.IsFaulted then tcs.SetException(t.Exception |> flattenExns)
      elif t.IsCanceled then tcs.SetCanceled ()
      else tcs.SetResult(())), TaskContinuationOptions.ExecuteSynchronously)
    |> ignore
    tcs.Task |> Async.AwaitTask |> rewrapAsyncExn

  type Microsoft.FSharp.Control.Async with
    static member AwaitTask t = awaitTask t

[<AutoOpen>]
module Primitives =

  // https://github.com/EventStore/EventStore/wiki/Writing-Events-%28.NET-API%29

  /// The specific event version to read as detailed on ReadEventAsync
  type EventVersion =
    | SpecificEvent of uint32
    /// -1 - read the last written event
    | LastInStream

  /// The version at which we currently expect the stream to be in order that an
  /// optimistic concurrency check can be performed. This should either be a 
  /// positive integer, or one of the constants ExpectedVersion.NoStream, 
  /// ExpectedVersion.EmptyStream, or to disable the check, ExpectedVersion.Any.
  /// The level of idempotence guarantee depends on whether or not the optimistic 
  /// concurrency check is not disabled during writing (by passing ExpectedVersion.Any
  /// as the expectedVersion for the write).
  /// More information at
  /// https://github.com/EventStore/EventStore/wiki/Optimistic-Concurrency-&-Idempotence
  type ExpectedVersionUnion =
    /// To be used when you don't have an empty stream nor when you have no stream,
    /// i.e. to be used when you have previously saved events to the stream and hence
    /// know its version.
    | Specific of uint32
    /// Use this if you create the stream when you start appending to it.
    | NoStream
    /// Use this for streams that have been created explicitly
    /// before appending to them (or deleting them)
    | EmptyStream
    /// Disable concurrency checks and just look at the commit id and
    /// data.
    | Any

  /// The strategy to use when encountering links in the 
  /// event stream.
  type ResolveLinksStrategy =
    | ResolveLinks
    | DontResolveLinks

  type StreamCheckpoint =
    | StreamStart
    /// Exclusive event number to subscribe from. I.e. you will not receive this event.
    | StreamEventNumber of uint32

module internal Helpers =

  let validUint (b : uint32) =
    if b > uint32(System.Int32.MaxValue) then
      failwithf "too large start %A, gt int32 max val" b
    else int b

  let action  f = new System.Action<_>(f)
  let action2 f = new System.Action<_, _>(f)
  let action3 f = new System.Action<_, _, _>(f)

  /// f' is a functor that maps origF's second argument from 'b to 'x
  let functor2 (fMap : 'x -> 'b) (origF : _ -> 'b -> 'c) : (_ -> 'x -> 'c) =
    fun a x -> origF a (fMap x)

  open EventStore.ClientAPI

  let (|EventVersion|) = function
    | SpecificEvent i -> int i
    | LastInStream    -> -1

  /// (Active-) pattern match out the GetEventStore integer value denoting each
  /// of the cases in the discriminated union ExpectedVersionUnion.
  let (|ExpectedVersion|) = function
    | Specific i  -> int i
    | EmptyStream -> ExpectedVersion.EmptyStream
    | NoStream    -> ExpectedVersion.NoStream
    | Any         -> ExpectedVersion.Any

  let (|ResolveLinks|) = function
    | ResolveLinks     -> true
    | DontResolveLinks -> false

  open System

  open EventStore.ClientAPI.Exceptions

  let canThrowWrongExpectedVersion awaitable =
    async {
      try
        let! res = awaitable
        return Choice1Of2(res)
      with :? WrongExpectedVersionException as ex ->
        return Choice2Of2 ex }

  let memoize f =
    let cache = ref Map.empty
    fun x ->
      match (!cache).TryFind(x) with
      | Some res -> res
      | None ->
        let res = f x
        cache := (!cache).Add(x,res)
        res

  // returns the constructor for the instance and a setter function, as a tuple, respectively
  let reflPkgFor<'a> () : (unit -> 'a) * (string -> obj -> 'a -> 'a) =
    let setterFor name = typeof<'a>.GetField(name, System.Reflection.BindingFlags.Instance ||| System.Reflection.BindingFlags.NonPublic ||| System.Reflection.BindingFlags.Public)
    let memSetterFor = memoize setterFor
    let emptyCtor = fun () -> System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof<'a>) :?> 'a
    let setter prop (value : obj) (instance : 'a) =
      if obj.ReferenceEquals(instance, null) then invalidArg "instance" "instance is null"
      let fi = memSetterFor prop
      let b = instance |> box
      if obj.ReferenceEquals(fi, null) then
        failwith <| sprintf "FieldInfo for property %s on type %s could not be found"
                      prop typeof<'a>.FullName
      fi.SetValue(b, value)
      b |> unbox<'a>
    emptyCtor, setter

  module Option =
    let fromNullable = function
      | _ : System.Nullable<'a> as n when n.HasValue -> Some <| n.Value
      | _ as n -> None

    let toNullable = function
      | Some item -> new System.Nullable<_>(item)
      | None      -> new System.Nullable<_>()

    /// foldObj f s inp evaluates to match in with null -> s | o -> Some <| f s o.
    let foldObj f s inp =
      match inp with
      | null -> s
      | o    -> Some <| f s o

    /// transform an option to a null or unwrap its value
    let noneIsNull = function
      | None -> null
      | Some x -> x

  // currently unused, because I can't get the types to match properly
  module Async =
    let map fMap value =
      async { let! v = value
              return fMap v }

    //   : (('a -> 'b) -> ('c -> Async<'a>) -> 'c -> Async<'b>)
    let mapCompose fMap fOrig c =
      async {
        let! res = fOrig c
        return fMap res }

// https://github.com/eulerfx/DDDInventoryItemFSharp/blob/master/DDDInventoryItemFSharp/EventStore.fs
// https://github.com/EventStore/getting-started-with-event-store/blob/master/src/GetEventStoreRepository/GetEventStoreRepository.cs
// https://github.com/EventStore/getting-started-with-event-store/blob/master/src/GetEventStoreRepository.Tests/GetEventStoreRepositoryIntegrationTests.cs

open System
open System.Net
open System.Threading.Tasks

type Count = uint32
type Offset =
    | Specific of uint32
    | LastInStream
type StreamId = string
type GroupName = string
type TransactionId = int64

type ConnectionDisconnected = delegate of obj * ClientConnectionEventArgs -> unit
and ConnectionReconnecting = delegate of obj * ClientReconnectingEventArgs -> unit
and ConnectionClosed = delegate of obj * ClientClosedEventArgs -> unit
and ConnectionError = delegate of obj * ClientErrorEventArgs -> unit
and ConnectionAuthenticationFailed = delegate of obj * ClientAuthenticationFailedEventArgs -> unit
and ConnectionConnected = delegate of obj * ClientConnectionEventArgs -> unit

/// Wrapper interface for <see cref="EventStore.ClientAPI.IEventStoreConnection" />
/// that has all the important operations.
/// Consumable from C# in order to allow easy mocking and stubbing.
/// See the 'Conn' module for documentation of each method.
and Connection =
  inherit IDisposable

  // events
  //abstract Diconnected                   : IEvent<ConnectionDisconnected, ClientConnectionEventArgs>
  //abstract Reconnecting                  : IEvent<ConnectionReconnecting, ClientReconnectingEventArgs>
  //abstract Closed                        : IEvent<ConnectionClosed, ClientClosedEventArgs>
  //abstract ErrorOccurred                 : IEvent<ConnectionError, ClientErrorEventArgs>
  //abstract AuthenticationFailed          : IEvent<ConnectionAuthenticationFailed, ClientAuthenticationFailedEventArgs>
  //abstract Connected                     : IEvent<ConnectionConnected, ClientConnectionEventArgs>

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L181
  abstract AppendToStream                : StreamId
                                            * ExpectedVersionUnion
                                            * SystemData.UserCredentials
                                            * EventData seq
                                            -> WriteResult Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L69
  abstract Close                         : unit -> unit

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L64
  abstract Connect                       : unit -> Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L53
  abstract ConnectionName                : string

  abstract ConnectPersistentSubscription : StreamId
                                            * GroupName
                                            * Action<EventStorePersistentSubscription, ResolvedEvent>
                                            // subscriptionDropped:
                                            * Action<EventStorePersistentSubscription, SubscriptionDropReason, exn> option
                                            * SystemData.UserCredentials
                                            * Count
                                            * bool
                                            -> EventStorePersistentSubscription

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L242
  abstract ContinueTransaction           : TransactionId
                                            * SystemData.UserCredentials
                                            -> EventStoreTransaction

  abstract CreatePersistentSubscription  : StreamId
                                            * GroupName
                                            * PersistentSubscriptionSettings
                                            * SystemData.UserCredentials
                                            -> Task

  abstract DeletePersistentSubscription  : StreamId
                                            * GroupName
                                            * SystemData.UserCredentials
                                            -> Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L86
  abstract DeleteStream                  : StreamId
                                            * ExpectedVersionUnion
                                            * SystemData.UserCredentials
                                            -> DeleteResult Task

  abstract GetStreamMetadata             : StreamId
                                            * SystemData.UserCredentials
                                            -> StreamMetadataResult Task

  abstract GetStreamMetadataBytes        : StreamId
                                            * SystemData.UserCredentials
                                            -> RawStreamMetadataResult Task

  // all read
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L346
  abstract ReadAllEventsBackward         : Position * Count * ResolveLinksStrategy
                                            * SystemData.UserCredentials
                                            -> AllEventsSlice Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L326
  abstract ReadAllEventsForward          : Position * Count * ResolveLinksStrategy
                                            * SystemData.UserCredentials
                                            -> AllEventsSlice Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L262
  abstract ReadEvent                     : StreamId
                                            * EventVersion
                                            * ResolveLinksStrategy
                                            * SystemData.UserCredentials
                                            -> EventReadResult Task

  // stream read
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L306
  abstract ReadStreamEventsBackward      : StreamId * Offset * Count
                                            * ResolveLinksStrategy
                                            * SystemData.UserCredentials
                                            -> StreamEventsSlice Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L284
  abstract ReadStreamEventsForward       : StreamId * Offset * Count
                                            * ResolveLinksStrategy
                                            * SystemData.UserCredentials
                                            -> StreamEventsSlice Task

  // metadata, skipping 'raw' methods so far
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L393
  abstract SetStreamMetadata             : StreamId
                                            * ExpectedVersionUnion
                                            * StreamMetadata
                                            * SystemData.UserCredentials
                                            -> WriteResult Task

  abstract SetStreamMetadataBytes        : StreamId
                                            * ExpectedVersionUnion
                                            * byte []
                                            * SystemData.UserCredentials
                                            -> WriteResult Task

  abstract SetSystemSettings             : SystemSettings
                                            * SystemData.UserCredentials
                                            -> Task

  // tx
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L229
  abstract StartTransaction              : StreamId
                                            * ExpectedVersionUnion
                                            * SystemData.UserCredentials
                                           -> EventStoreTransaction Task

  // subscribe API

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L377
  abstract SubscribeToAll                : ResolveLinksStrategy
                                            // eventAppeared:
                                            * Action<EventStoreSubscription, ResolvedEvent>
                                            // subscriptionDropped:
                                            * Action<EventStoreSubscription, SubscriptionDropReason, exn> option
                                            * SystemData.UserCredentials
                                            -> EventStoreSubscription Task

  /// Subscribe from, position exclusive
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L383
  abstract SubscribeToAllFrom            : Position option
                                            * ResolveLinksStrategy
                                            // eventAppeared:
                                            * Action<EventStoreCatchUpSubscription, ResolvedEvent>
                                            // liveProcessingStarted:
                                            * Action<EventStoreCatchUpSubscription> option
                                            // subscriptionDropped:
                                            * Action<EventStoreCatchUpSubscription, SubscriptionDropReason, exn> option
                                            * SystemData.UserCredentials
                                            -> EventStoreAllCatchUpSubscription

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L355
  abstract SubscribeToStream             : StreamId
                                            // resolveLinks:
                                            * ResolveLinksStrategy
                                            // eventAppeared:
                                            * Action<EventStoreSubscription, ResolvedEvent>
                                            // subscriptionDropped:
                                            * Action<EventStoreSubscription, SubscriptionDropReason, exn> option
                                            * SystemData.UserCredentials
                                            -> EventStoreSubscription Task

  /// Subscribe from, offset exclusive
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L362
  abstract SubscribeToStreamFrom         : StreamId
                                            * StreamCheckpoint
                                            // resolveLinks:
                                            * ResolveLinksStrategy
                                            // eventAppeared:
                                            * Action<EventStoreCatchUpSubscription, ResolvedEvent>
                                            // liveProcessingStarted:
                                            * Action<EventStoreCatchUpSubscription> option
                                            // subscriptionDropped:
                                            * Action<EventStoreCatchUpSubscription, SubscriptionDropReason, exn> option
                                            * SystemData.UserCredentials
                                            -> EventStoreStreamCatchUpSubscription

  abstract UpdatePersistentSubscription  : StreamId
                                            * GroupName
                                            * PersistentSubscriptionSettings
                                            * SystemData.UserCredentials
                                            -> Task

/// Type extensions that allow the programmer to easily cast the 
/// <see cref="EventStore.ClientAPI.EventStoreConnection" /> to
/// a type more easily stubbed and mocked.
[<AutoOpen>]
module TypeExtensions =

  open Helpers
  open AsyncHelpers

  type EventStore.ClientAPI.IEventStoreConnection with
    /// Convert the connection to be usable with the F# API (an interface
    /// to allow easy stubbing and mocking).
    member y.ApiWrap() : Connection =
      { new Connection with

          //member Diconnected = y.Disconnected
          //member Reconnecting = y.Reconnecting
          //member Closed = y.Closed
          //member ErrorOccurred = y.ErrorOccurred
          //member AuthenticationFailed = y.AuthenticationFailed
          //member Connected = y.Connected

          member x.AppendToStream(streamId, expectedVersion, userCredentials, events) =
            match expectedVersion with
            | ExpectedVersion expectedVersion ->
              y.AppendToStreamAsync(streamId, expectedVersion, events, userCredentials)

          member x.ConnectionName =
            y.ConnectionName

          member x.Close () =
            y.Close()

          member x.Connect () =
            y.ConnectAsync ()

          member x.ConnectPersistentSubscription (streamId, groupName, eventAppeared, subscriptionDropped, userCredentials, bufferSize, autoAck) =
            let bufferSize = validUint bufferSize
            y.ConnectToPersistentSubscription(streamId, groupName, eventAppeared,
                                              Option.noneIsNull subscriptionDropped, userCredentials,
                                              bufferSize, autoAck)

          member x.ContinueTransaction(txId, userCredentials) =
            y.ContinueTransaction(txId, userCredentials)

          member x.CreatePersistentSubscription(streamId, groupName, settings, userCredentials) =
            y.CreatePersistentSubscriptionAsync(streamId, groupName, settings, userCredentials)

          member x.DeletePersistentSubscription(streamId, groupName, userCredentials) =
            y.DeletePersistentSubscriptionAsync(streamId, groupName, userCredentials)

          member x.DeleteStream (streamId, expectedVersion, userCredentials) =
            match expectedVersion with
            | ExpectedVersion expectedVersion ->
              y.DeleteStreamAsync(streamId, expectedVersion, userCredentials)

          member x.GetStreamMetadata (streamId, userCredentials) =
            y.GetStreamMetadataAsync(streamId, userCredentials)

          member x.GetStreamMetadataBytes(streamId, userCredentials) =
            y.GetStreamMetadataAsRawBytesAsync(streamId, userCredentials)

          member x.ReadAllEventsBackward (position, maxCount, resolveLinks, userCredentials) =
            let maxCount = validUint maxCount
            y.ReadAllEventsBackwardAsync(position, maxCount,
                                         (match resolveLinks with ResolveLinks r -> r),
                                         userCredentials)

          member x.ReadAllEventsForward (position, maxCount, resolveLinks, userCredentials) =
            let maxCount = validUint maxCount
            y.ReadAllEventsForwardAsync(position, maxCount,
                                        (match resolveLinks with ResolveLinks r -> r),
                                        userCredentials)

          member x.ReadEvent(stream, eventVersion, resolveLinks, userCredentials) =
            match eventVersion with
            | EventVersion eventVersion ->
              y.ReadEventAsync(stream, eventVersion,
                               (match resolveLinks with ResolveLinks r -> r),
                               userCredentials)

          member x.ReadStreamEventsForward (streamId, start, count, resolveLinks, userCredentials) =
            let count = validUint count
            let startEvent =
                match start with
                | Specific i -> validUint i
                | LastInStream -> -1
            y.ReadStreamEventsForwardAsync(streamId, startEvent, count,
                                           (match resolveLinks with ResolveLinks r -> r),
                                           userCredentials)

          member x.ReadStreamEventsBackward (streamId, start, count, resolveLinks, userCredentials) =
            let count = validUint count
            let startEvent =
                match start with
                | Specific i -> validUint i
                | LastInStream -> -1
            y.ReadStreamEventsBackwardAsync(streamId, startEvent, count,
                                            (match resolveLinks with ResolveLinks r -> r),
                                            userCredentials)

          member x.SetStreamMetadata (streamId, expectedVersion, metadata, userCredentials) =
            match expectedVersion with
            | ExpectedVersion expectedVersion ->
              y.SetStreamMetadataAsync(streamId, expectedVersion, metadata, userCredentials)

          member x.SetStreamMetadataBytes(streamId, expectedVersion, metadata, userCredentials) =
            match expectedVersion with
            | ExpectedVersion expectedVersion ->
              y.SetStreamMetadataAsync(streamId, expectedVersion, metadata, userCredentials)

          member x.SetSystemSettings(systemSettings, userCredentials) =
            y.SetSystemSettingsAsync(systemSettings, userCredentials)

          member x.StartTransaction (streamId, expectedVersion, userCredentials) =
            match expectedVersion with
            | ExpectedVersion expectedVersion ->
              y.StartTransactionAsync(streamId, expectedVersion, userCredentials)
            
          member x.SubscribeToAll (resolveLinks, eventAppeared, subscriptionDropped,
                                        userCredentials) =
            let resolveLinks = match resolveLinks with ResolveLinks r -> r
            y.SubscribeToAllAsync(resolveLinks, eventAppeared,
                                  Option.noneIsNull subscriptionDropped,
                                  userCredentials)

          member x.SubscribeToAllFrom (fromPos, resolveLinks, eventAppeared,
                                       liveProcessingStarted, subscriptionDropped, userCredentials) =
            let resolveLinks = match resolveLinks with ResolveLinks r -> r
            y.SubscribeToAllFrom(Option.toNullable fromPos,
                                 resolveLinks, eventAppeared,
                                 Option.noneIsNull liveProcessingStarted,
                                 Option.noneIsNull subscriptionDropped,
                                 userCredentials)

          member x.SubscribeToStream (streamId, resolveLinks, eventAppeared, subscriptionDropped, userCredentials) =
            let resolveLinks = match resolveLinks with ResolveLinks r -> r
            y.SubscribeToStreamAsync(streamId, resolveLinks, eventAppeared,
                                     Option.noneIsNull subscriptionDropped,
                                     userCredentials)

          member x.SubscribeToStreamFrom (streamId, lastCheckpoint, resolveLinks,
                                          eventAppeared, liveProcessingStarted,
                                          subscriptionDropped, userCredentials) =
            let lastCheckpoint =
              match lastCheckpoint with
              | StreamStart -> EventStore.ClientAPI.StreamCheckpoint.StreamStart
              | StreamEventNumber n -> Nullable<int> (int n)
            let resolveLinks = match resolveLinks with ResolveLinks r -> r
            y.SubscribeToStreamFrom(streamId,
                                    lastCheckpoint,
                                    resolveLinks, eventAppeared,
                                    Option.noneIsNull liveProcessingStarted,
                                    Option.noneIsNull subscriptionDropped,
                                    userCredentials)

          member x.UpdatePersistentSubscription(streamId, groupName, subscriptionSettings, userCredentials) =
            y.UpdatePersistentSubscriptionAsync(streamId, groupName, subscriptionSettings, userCredentials)

          member x.Dispose() =
            y.Dispose()
          }

[<AutoOpen>]
module Types =

  open Helpers
  open System
  open System.Reflection
  open EventStore.ClientAPI

  type private Empty =
    static member ByteArray = [||]
    static member ResolvedEvents = [||]

  // EVENT DATA

  let private emptyEventData = EventData(Guid.Empty, "", true, Empty.ByteArray, Empty.ByteArray)

  type EventStore.ClientAPI.EventData with
    static member Empty = emptyEventData

  /// <see cref="EventStore.ClientAPI.EventData" />
  type EventData =
    { Id       : System.Guid
      Type     : string
      Metadata : byte array
      Data     : byte array
      IsJson   : bool }

  let unwrapEventData (e : EventData) =
    EventStore.ClientAPI.EventData(e.Id, e.Type, e.IsJson, e.Data, e.Metadata)

  let wrapEventData (e : EventStore.ClientAPI.EventData) =
    { Id       = e.EventId
      Type     = e.Type
      Metadata = e.Metadata
      Data     = e.Data
      IsJson   = e.IsJson }

  type EventData with
    static member Empty = EventStore.ClientAPI.EventData.Empty |> wrapEventData

  // RECORDED EVENT

  let private ctorRecordedEvent, private setterRecE = reflPkgFor<EventStore.ClientAPI.RecordedEvent> ()

  let private newRecordedEvent (eventId : Guid) (createdEpoch : int64) (streamId : string) (evtType : string) (number : int) (data : byte array) (metaData : byte array) (isJson : bool) =
    ctorRecordedEvent ()
    |> setterRecE "CreatedEpoch" createdEpoch
    |> setterRecE "EventStreamId" streamId
    |> setterRecE "EventId" eventId
    |> setterRecE "EventNumber" number
    |> setterRecE "EventType" evtType
    |> setterRecE "Data" data
    |> setterRecE "Metadata" metaData
    |> setterRecE "IsJson" isJson

  let private emptyRecordedEvent =
    newRecordedEvent Guid.Empty 0L "" "" 0 Empty.ByteArray Empty.ByteArray false

  type EventStore.ClientAPI.RecordedEvent with
    /// Gets an empty recorded event, for interop purposes.
    static member Empty = emptyRecordedEvent

  type RecordedEvent =
    { Id       : System.Guid
      /// ms since epoch
      Created  : int64
      StreamId : string
      Type     : string
      Metadata : byte array
      Data     : byte array
      Number   : uint32
      IsJson   : bool }
    override x.ToString() =
      sprintf "%A" x
    static member FromEventData streamId number created (evt : EventData) =
      { Id       = evt.Id
        Created  = created
        StreamId = streamId
        Type     = evt.Type
        Metadata = evt.Metadata
        Data     = evt.Data
        Number   = number
        IsJson   = evt.IsJson }

  let unwrapRecordedEvent (e : RecordedEvent) =
    let number = validUint e.Number
    newRecordedEvent e.Id e.Created e.StreamId e.Type number e.Data e.Metadata e.IsJson

  let wrapRecordedEvent (e : EventStore.ClientAPI.RecordedEvent) =
    if obj.ReferenceEquals(e, null) then invalidArg "e" "e is null"
    { Id       = e.EventId
      Created  = e.CreatedEpoch
      StreamId = e.EventStreamId
      Type     = e.EventType
      Metadata = e.Metadata
      Data     = e.Data
      Number   = e.EventNumber |> uint32
      IsJson   = e.IsJson }

  type RecordedEvent with
    static member Empty = EventStore.ClientAPI.RecordedEvent.Empty |> wrapRecordedEvent

  type EventData with
    static member FromRecordedEvent isJson (evt : RecordedEvent) : EventData =
      { Id       = evt.Id
        Type     = evt.Type
        Metadata = evt.Metadata
        Data     = evt.Data
        IsJson   = isJson }

  //////////// RESOLVED EVENT ////////////////

  let private ctorRE, private setRE = reflPkgFor<EventStore.ClientAPI.ResolvedEvent> ()

  let private newResolvedEvent
    (event : EventStore.ClientAPI.RecordedEvent)
    (link : EventStore.ClientAPI.RecordedEvent)
    (originalPosition : System.Nullable<Position>) =
    ctorRE ()
    |> setRE "Event" event
    |> setRE "Link" link
    |> setRE "OriginalPosition" originalPosition

  let private emptyPosition =
    new System.Nullable<Position>()

  let internal emptyResolvedEvent =
    newResolvedEvent emptyRecordedEvent emptyRecordedEvent emptyPosition

  type EventStore.ClientAPI.ResolvedEvent with
    /// Gets an empty event store ResolvedEvent, for iwnterop purposes.
    static member Empty = emptyResolvedEvent

  /// For docs <see cref="EventStore.ClientAPI.ResolvedEvent" />
  type ResolvedEvent =
    { Event            : RecordedEvent
      Link             : RecordedEvent option
      OriginalPosition : Position option }
    /// By default takes the Link-recorded event, or if there's no link,
    /// takes the event itself
    member x.OriginalEvent =
      x.Link |> Option.fold (fun s t -> t) x.Event
    /// By default takes the Link-recorded event stream id, or if there's
    /// no link, takes the event's stream id itself
    member x.OriginalStreamId =
      x.OriginalEvent.StreamId
    /// By default takes the Link-recorded event number, or otherwise, if
    /// there's no link, takes the event's actual number.
    member x.OriginalEventNumber =
      x.OriginalEvent.Number
    override x.ToString() =
      sprintf "%A" x

  let unwrapResolvedEvent (e : ResolvedEvent) =
    newResolvedEvent (e.Event |> unwrapRecordedEvent)
                     (e.Link |> Option.fold (fun s -> unwrapRecordedEvent) null)
                     (Option.toNullable e.OriginalPosition)

  /// Convert a <see cref="EventStore.ClientAPI.ResolvedEveRent" /> to a
  /// DataModel.EventRef.
  let wrapResolvedEvent (e : EventStore.ClientAPI.ResolvedEvent) =
    { Event            = e.Event |> wrapRecordedEvent
      Link             = e.Link  |> Option.foldObj (fun s -> wrapRecordedEvent) None
      OriginalPosition = e.OriginalPosition |> Option.fromNullable }

  type ResolvedEvent with
    static member Empty = EventStore.ClientAPI.ResolvedEvent.Empty |> wrapResolvedEvent

  //////////// EVENT READ RESULT ////////////////

  let private ctorERS, private setERS = reflPkgFor<EventStore.ClientAPI.EventReadResult> ()

  let private newEventReadResult
    (status : EventStore.ClientAPI.EventReadStatus)
    (stream : string)
    (eventNumber : int)
    (event : EventStore.ClientAPI.ResolvedEvent Nullable) =
    ctorERS ()
    |> setERS "Status" status
    |> setERS "Stream" stream
    |> setERS "EventNumber" eventNumber
    |> setERS "Event" event

  let private emptyEventReadResult =
    newEventReadResult EventReadStatus.Success "" -1 (new System.Nullable<EventStore.ClientAPI.ResolvedEvent>())

  type EventStore.ClientAPI.EventReadResult with
    static member Empty = emptyEventReadResult

  type EventReadResult =
    { Status      : EventReadStatus
      Stream      : StreamId
      EventNumber : int
      Event       : ResolvedEvent option }

  let unwrapEventReadResult (e : EventReadResult) =
    let nn x = new Nullable<_>(x)
    newEventReadResult e.Status e.Stream e.EventNumber
      (e.Event |> Option.fold (fun _ -> nn << unwrapResolvedEvent) (new Nullable<_>()))

  let wrapEventReadResult (e : EventStore.ClientAPI.EventReadResult) =
    { Status      = e.Status
      Stream      = e.Stream
      EventNumber = e.EventNumber
      Event       = e.Event |> Option.fromNullable |> Option.map wrapResolvedEvent }

  type EventReadResult with
    static member Empty = EventStore.ClientAPI.EventReadResult.Empty |> wrapEventReadResult

  //////////// READ DIRECTION //////////////

  type ReadDirection =
    | Forward
    | Backward

  let wrapReadDirection =
    let back = EventStore.ClientAPI.ReadDirection.Backward
    function
    | _ as rd when (rd : EventStore.ClientAPI.ReadDirection) = back -> Backward
    | _ -> Forward

  let unwrapReadDirection = function
    | Backward -> EventStore.ClientAPI.ReadDirection.Backward
    | _        -> EventStore.ClientAPI.ReadDirection.Forward

  // STREAM EVENTS SLICE

  let private ctorSES, private setterSES = reflPkgFor<EventStore.ClientAPI.StreamEventsSlice> ()

  let private newSES (srs : EventStore.ClientAPI.SliceReadStatus)
    (stream : string)
    (fromEventNumber : int)
    (readDirection : EventStore.ClientAPI.ReadDirection)
    (events : EventStore.ClientAPI.ResolvedEvent array)
    (nextEventNumber : int)
    (lastEventNumber : int)
    (isEndOfStream : bool) =
    ctorSES ()
    |> setterSES "Status" srs
    |> setterSES "Stream" stream
    |> setterSES "FromEventNumber" fromEventNumber
    |> setterSES "ReadDirection" readDirection
    |> setterSES "Events" events
    |> setterSES "NextEventNumber" nextEventNumber
    |> setterSES "LastEventNumber" lastEventNumber
    |> setterSES "IsEndOfStream" isEndOfStream

  let private emptySES status streamId =
    newSES status streamId 0 EventStore.ClientAPI.ReadDirection.Forward Empty.ResolvedEvents 0 0 true

  type EventStore.ClientAPI.StreamEventsSlice with
    /// Gets an empty event store ResolvedEvent, for interop purposes.
    static member Empty = emptySES EventStore.ClientAPI.SliceReadStatus.Success "empty-stream"

  /// <see cref="EventStore.ClientAPI.StreamEventsSlice" />
  type StreamEventsSlice =
    | NotFound of StreamId
    | Deleted of StreamId
    | Success of StreamSlice
  /// Convert a <see cref="EventStore.ClientAPI.StreamEventsSlice" /> to a
  /// ConnectionApi.EventsSlice.
  and StreamSlice =
    | StreamSlice of Slice
    | EndOfStream of Slice
  and Slice =
    { Events          : ResolvedEvent list
      Stream          : StreamId
      FromEventNumber : uint32
      ReadDirection   : ReadDirection
      NextEventNumber : uint32
      LastEventNumber : uint32 }
  and StreamId = string

  let unwrapStreamEventsSlice =
    let createSES slice =
      let fromEvtNo = validUint slice.FromEventNumber
      let nextEvtNo = validUint slice.NextEventNumber
      let direction = slice.ReadDirection |> unwrapReadDirection
      let events = slice.Events |> List.map unwrapResolvedEvent |> List.toArray
      let lastEvtNo = validUint slice.LastEventNumber
      newSES SliceReadStatus.Success slice.Stream fromEvtNo direction events nextEvtNo lastEvtNo
    function
    | NotFound streamId ->
      emptySES SliceReadStatus.StreamNotFound streamId
    | Deleted streamId ->
      emptySES SliceReadStatus.StreamDeleted streamId
    | Success(EndOfStream slice) -> createSES slice true
    | Success(StreamSlice slice) -> createSES slice false

  let wrapStreamEventsSlice = function
    | _ as s when (s : EventStore.ClientAPI.StreamEventsSlice).Status = SliceReadStatus.StreamDeleted ->
      Deleted s.Stream
    | _ as s when s.Status = SliceReadStatus.StreamNotFound ->
      NotFound s.Stream
    | _ as s ->
      let data = { Events          = s.Events |> List.ofArray |> List.map wrapResolvedEvent
                 ; Stream          = s.Stream
                 ; FromEventNumber = uint32(s.FromEventNumber)
                 ; ReadDirection   = s.ReadDirection |> wrapReadDirection
                 ; NextEventNumber = uint32(s.NextEventNumber)
                 ; LastEventNumber = uint32(s.LastEventNumber) }
      Success <| (if s.IsEndOfStream then EndOfStream else StreamSlice) data

  type StreamEventsSlice with
    static member Empty =
      emptySES EventStore.ClientAPI.SliceReadStatus.Success "empty-stream" |> wrapStreamEventsSlice

  // ALL EVENTS SLICE

  let private ctorAES, private setterAES = reflPkgFor<EventStore.ClientAPI.AllEventsSlice> ()

  let private newAES (readDirection : EventStore.ClientAPI.ReadDirection)
    (fromPosition : Position)
    (nextPosition : Position)
    (events : EventStore.ClientAPI.ResolvedEvent array)
    (isEndOfStream : bool) =
    ctorAES ()
    |> setterAES "ReadDirection" readDirection
    |> setterAES "FromPosition" fromPosition
    |> setterAES "NextPosition" nextPosition
    |> setterAES "Events" events

  let private emptyAES () =
    let emptyPosition = EventStore.ClientAPI.Position()
    let forward = EventStore.ClientAPI.ReadDirection.Forward
    newAES forward emptyPosition emptyPosition [||]

  type EventStore.ClientAPI.AllEventsSlice with
    /// Gets an empty event store ResolvedEvent, for interop purposes.
    static member Empty = emptyAES ()

  /// <see cref="EventStore.ClientAPI.AllEventsSlice" />
  type AllEventsSlice = 
    { Events        : ResolvedEvent list
      FromPosition  : Position
      NextPosition  : Position
      ReadDirection : ReadDirection
      IsEndOfStream : bool }

  type AllEventsSlice with
    static member Empty =
      { Events        = []
        FromPosition  = Position()
        NextPosition  = Position()
        ReadDirection = Forward
        IsEndOfStream = true }

  let unwrapAllEventsSlice (e : AllEventsSlice) =
    newAES (unwrapReadDirection e.ReadDirection)
           e.FromPosition
           e.NextPosition
           (e.Events |> List.map unwrapResolvedEvent |> List.toArray)

  let wrapAllEventsSlice (e : EventStore.ClientAPI.AllEventsSlice) =
    { Events        = e.Events |> List.ofArray |> List.map wrapResolvedEvent
      FromPosition  = e.FromPosition
      NextPosition  = e.FromPosition
      ReadDirection = e.ReadDirection |> wrapReadDirection
      IsEndOfStream = e.IsEndOfStream }

/// Thin F# wrapper on top of EventStore.Client's API. Doesn't
/// translate any structures, but enables currying and F# calling conventions.
/// The EventStoreConnection class is responsible for maintaining a full-duplex connection between the client and the event store server. EventStoreConnection is thread-safe, and it is recommended that only one instance per application is created.
/// This module has its methods sorted by the 'Connection' interface.
module Conn =

  open Helpers
  open AsyncHelpers

  // CONNECTION API

  let name (c : Connection) = c.ConnectionName

  /// AppendToStreamAsync:
  /// see docs for append.
  let internal appendRaw (c : Connection)
                         streamId
                         expectedVersion credentials
                         (eventData : EventStore.ClientAPI.EventData list) =
    c.AppendToStream(streamId, expectedVersion, credentials, eventData)
    |> awaitTask
    |> canThrowWrongExpectedVersion

  /// <summary><para>
  /// AppendToStreamAsync:
  /// If expectedVersion == currentVersion - events will be written and acknowledged.
  /// If expectedVersion &lt; currentVersion - the EventId of each event in the stream starting from
  /// expectedVersion are compared to those in the write operation.
  /// This can yield one of three further results:
  /// </para><list>
  ///   <item>All events have been committed already - the write will be acknowledged as successful,
  ///   but no duplicate events will be written.</item>
  ///   <item>None of the events were previously committed - a WrongExpectedVersionException will be thrown.</item>
  ///   <item>Some of the events were previously committed - this is considered a bad request.
  ///   If the write contains the same events as a previous request, either all or none
  ///   of the events should have been previously committed. This currently surfaces as
  ///   a WrongExpectedVersionException.</item>
  /// </list><para>
  /// Success is denoted in Choice1Of2 with a unit in it.
  /// </para></summary>
  /// <remarks><para>
  /// When appending events to a stream the <see cref="ExpectedVersion"/> choice can
  /// make a very large difference in the observed behavior. For example, if no stream exists
  /// and ExpectedVersion.Any is used, a new stream will be implicitly created when appending.
  /// </para><para>
  /// There are also differences in idempotency between different types of calls.
  /// If you specify an ExpectedVersion aside from ExpectedVersion.Any the Event Store
  /// will give you an idempotency guarantee. If using ExpectedVersion.Any the Event Store
  /// will do its best to provide idempotency but does not guarantee idempotency.
  /// </para></remarks>
  /// <exception cref="EventStore.ClientAPI.Exceptions.WrongExpectedVersionException">
  /// If expectedVersion > currentVersion - a WrongExpectedVersionException will be returned in Choice2Of2.
  /// If None of the events were previously committed - a WrongExpectedVersionException will be returned in Choice2Of2.
  /// </exception>
  let append (c : Connection) credentials streamId expectedVersion
             (eventData : EventData list) =
    appendRaw c streamId expectedVersion credentials (eventData |> List.map unwrapEventData)

  /// <summary>
  /// SubscribeToStream
  /// Asynchronously subscribes to a single event stream. New events
  /// written to the stream while the subscription is active will be
  /// pushed to the client.
  /// </summary>
  /// <param name="stream">The stream to subscribe to</param>
  /// <param name="resolveLinkTos">Whether to resolve Link events automatically</param>
  /// <param name="eventAppeared">An action invoked when a new event is received over the subscription</param>
  /// <param name="subscriptionDropped">An action invoked if the subscription is dropped</param>
  /// <param name="userCredentials">User credentials to use for the operation</param>
  /// <returns>An <see cref="EventStoreSubscription"/> representing the subscription</returns>
  let catchUp (c : Connection)
              streamId
              resolveLinks
              eventAppeared
              subscriptionDropped
              userCredentials =
    c.SubscribeToStream(streamId, resolveLinks,
                             eventAppeared |> functor2 Types.wrapResolvedEvent |> action2,
                             subscriptionDropped |> Option.map action3,
                             userCredentials)
    |> Async.AwaitTask

  [<Obsolete "use catchUp">]
  let subscribe = catchUp

  /// SubscribeToStreamFrom
  ///
  /// - `c` - event store connection
  /// - `streamId` - stream id to subscribe to
  /// - `streamCheckpoint` - where to start reading from
  /// - `resolveLinks` - ResolveLinksStategy
  /// - `eventAppeared` - the generic event/message handler
  /// - `liveProcessingStarted` - callback that's called when all historical events
  ///   have been processed and the subscription switches to 'live' mode
  /// - `subscriptionDropped` - for handling the drop of a subscription
  /// - `userCredentials` - user credentials to use for connecting to ES
  ///
  let catchUpFrom (c : Connection)
                  streamId
                  streamCheckpoint
                  resolveLinks
                  eventAppeared
                  liveProcessingStarted // EventStoreCatchUpSubscription -> unit
                  subscriptionDropped
                  userCredentials =
    c.SubscribeToStreamFrom(streamId, streamCheckpoint, resolveLinks,
                            eventAppeared |> functor2 Types.wrapResolvedEvent |> action2,
                            liveProcessingStarted |> Option.map action,
                            subscriptionDropped |> Option.map action3,
                            userCredentials)

  [<Obsolete "use catchUpFrom">]
  let subscribeFrom = catchUpFrom

  /// SubscribeToAll
  let catchUpAll (c : Connection)
                 resolveLinks
                 eventAppeared
                 subscriptionDropped
                 userCredentials =
    c.SubscribeToAll(resolveLinks,
                     eventAppeared |> functor2 Types.wrapResolvedEvent |> action2,
                     subscriptionDropped |> Option.map action3,
                     userCredentials)
    |> Async.AwaitTask

  [<Obsolete "use catchUpAll">]
  let subscribeAll = catchUpAll

  /// SubscribeToAllFrom
  let catchUpAllFrom (c : Connection) fromPos resolveLinks eventAppeared
    liveProcessingStarted subscriptionDropped userCredentials =
    c.SubscribeToAllFrom(fromPos, resolveLinks,
                         eventAppeared |> functor2 Types.wrapResolvedEvent |> action2,
                         liveProcessingStarted |> Option.map action,
                         subscriptionDropped |> Option.map action3,
                         userCredentials)

  [<Obsolete "use catchUpAllFrom">]
  let subscribeAllFrom = catchUpAllFrom

  /// Closes the EventStore connection
  let close (c : Connection) =
    c.Close()

  /// Connects the connection to the given connection string/uri.
  let connect (c : Connection) =
    if obj.ReferenceEquals(null, c) then invalidArg "c" "c is null"
    c.Connect() |> awaitTask

  /// <summary>
  /// Subscribes a persistent subscription (competing consumer) to the event store
  /// </summary>
  /// <param name="groupName">The subscription group to connect to</param>
  /// <param name="stream">The stream to subscribe to</param>
  /// <param name="eventAppeared">An action invoked when an event appears</param>
  /// <param name="subscriptionDropped">An action invoked if the subscription is dropped</param>
  /// <param name="userCredentials">User credentials to use for the operation</param>
  /// <param name="bufferSize">The buffer size to use for the persistent subscription</param>
  /// <param name="autoAck">Whether the subscription should automatically acknowledge messages processed.
  /// If not set the receiver is required to explicitly acknowledge messages through the subscription.</param>
  /// <remarks>This will connect you to a persistent subscription group for a stream. The subscription group
  /// must first be created with Conn.createPersistent many connections
  /// can connect to the same group and they will be treated as competing consumers within the group.
  /// If one connection dies work will be balanced across the rest of the consumers in the group. If
  /// you attempt to connect to a group that does not exist you will be given an exception.
  /// </remarks>
  /// <returns>An <see cref="EventStoreSubscription"/> representing the subscription</returns>
  let connectPersistent (c : Connection) streamId groupName eventAppeared
                        subscriptionDropped userCredentials bufferSize autoAck =
    c.ConnectPersistentSubscription(streamId, groupName,
                                    eventAppeared |> functor2 Types.wrapResolvedEvent |> action2,
                                    subscriptionDropped |> Option.map action3,
                                    userCredentials, bufferSize, autoAck)

  /// <summary>
  /// ContinueTransaction
  /// Continues transaction by provided transaction ID.
  /// </summary>
  /// <remarks>
  /// A <see cref="EventStoreTransaction"/> allows the calling of multiple writes with multiple
  /// round trips over long periods of time between the caller and the event store. This method
  /// is only available through the TCP interface and no equivalent exists for the RESTful interface.
  /// </remarks>
  /// <param name="transactionId">The transaction ID that needs to be continued.</param>
  /// <param name="userCredentials">The optional user credentials to perform operation with.</param>
  /// <returns><see cref="EventStoreTransaction"/> object.</returns>
  let continueTransaction (c : Connection) txId =
    c.ContinueTransaction txId

  /// <summary>
  /// Asynchronously create a persistent subscription group on a stream
  /// </summary>
  /// <param name="stream">The name of the stream to create the persistent subscription on</param>
  /// <param name="groupName">The name of the group to create</param>
  /// <param name="settings">The <see cref="PersistentSubscriptionSettings"></see> for the subscription</param>
  /// <param name="credentials">The credentials to be used for this operation.</param>
  /// <returns>A <see cref="PersistentSubscriptionCreateResult"/>.</returns>
  let createPersistent (c : Connection) streamId groupName settings userCredentials =
    c.CreatePersistentSubscription(streamId, groupName, settings, userCredentials)
    |> awaitTask

  /// DeleteStreamAsync
  let delete (c : Connection) expectedVersion streamId userCredentials =
    c.DeleteStream(streamId, expectedVersion, userCredentials)
    |> awaitTask
    |> canThrowWrongExpectedVersion

  /// DeletePersistentSubscription
  let deletePersistent (c : Connection) streamId groupName userCredentials =
    c.DeletePersistentSubscription(streamId, groupName, userCredentials)
    |> awaitTask

  /// GetStreamMetadata
  let getMetadata (c : Connection) streamId userCredentials =
    c.GetStreamMetadata(streamId, userCredentials)
    |> Async.AwaitTask

  /// GetStreamMetadataBytes
  let getMetadataBytes (c : Connection) streamId userCredentials =
    c.GetStreamMetadataBytes(streamId, userCredentials)
    |> Async.AwaitTask

  /// ReadAllEventsBackwardAsync
  let readAllBack (c : Connection) position maxCount resolveLinks userCredentials =
    c.ReadAllEventsBackward(position, maxCount, resolveLinks, userCredentials)
    |> Async.AwaitTask
    |> Async.map wrapAllEventsSlice

  /// ReadAllEventsForwardAsync
  let readAll (c : Connection) position maxCount resolveLinks userCredentials =
    c.ReadAllEventsForward(position, maxCount, resolveLinks, userCredentials)
    |> Async.AwaitTask
    |> Async.map wrapAllEventsSlice

  /// ReadEventAsync
  let internal readEventRaw (c : Connection) stream expectedVersion resolveLinks userCredentials =
    c.ReadEvent(stream, expectedVersion, resolveLinks, userCredentials)
    |> Async.AwaitTask

  /// ReadEventAsync
  let readEvent (c : Connection) stream expectedVersion resolveLinks userCredentials =
    readEventRaw c stream expectedVersion resolveLinks userCredentials
    |> Async.map wrapEventReadResult

  /// ReadStreamEventsBackwardAsync
  let internal readBackRaw (c : Connection) streamId start count resolveLinks userCredentials =
    c.ReadStreamEventsBackward(streamId, start, count, resolveLinks, userCredentials)
    |> Async.AwaitTask

  /// ReadStreamEventsBackwardAsync
  let readBack (c : Connection) streamId start count resolveLinks userCredentials =
    readBackRaw c streamId start count resolveLinks userCredentials
    |> Async.map wrapStreamEventsSlice

  /// ReadStreamEventsForwardAsync
  /// <param name="start">Where to start reading, inclusive</param>
  let internal readRaw (c : Connection) streamId start count resolveLinkTos userCredentials =
    c.ReadStreamEventsForward(streamId, start, count, resolveLinkTos, userCredentials)
    |> Async.AwaitTask

  /// ReadStreamEventsForwardAsync
  /// <param name="start">Where to start reading, inclusive</param>
  let read (c : Connection) streamId start count resolveLinkTos userCredentials =
    readRaw c streamId start count resolveLinkTos userCredentials
    |> Async.map wrapStreamEventsSlice

  /// SetStreamMetadataAsync
  let setMetadata (c : Connection) streamId expectedVersion metadata userCredentials =
    c.SetStreamMetadata(streamId, expectedVersion, metadata, userCredentials)
    |> awaitTask

  let setSystemSettings (c : Connection) settings userCredentials =
    c.SetSystemSettings(settings, userCredentials)
    |> awaitTask

  /// StartTransactionAsync
  let startTx streamId expectedVersion userCredentials (c : Connection) =
    c.StartTransaction(streamId, expectedVersion, userCredentials)
    |> Async.AwaitTask
    |> canThrowWrongExpectedVersion

  let updatePersistent (c : Connection) streamId groupName settings userCredentials =
    c.UpdatePersistentSubscription(streamId, groupName, settings, userCredentials)
    |> awaitTask

module ConnectionSettings =
  // BUILDING CONNECTION

  /// Create a builder and use the module ConnBuilder to set the settings.
  /// Call 'configureEnd' when you are done, to get the connection.
  let configureStart () =
    ConnectionSettings.Create()

  let configureEndEp (connectionUri : Uri) (settingBuilder : ConnectionSettingsBuilder) =
    let settings = ConnectionSettingsBuilder.op_Implicit(settingBuilder)
    let conn = EventStoreConnection.Create(settings, connectionUri)
    conn.ApiWrap()

  /// End configuring the connection settings and return
  /// a new connection with those settings (not connected).
  let configureEnd (endpoint : IPEndPoint) (settingBuilder : ConnectionSettingsBuilder) = //, clusterBuilder : ClusterSettingsBuilder) =
    let settings = ConnectionSettingsBuilder.op_Implicit(settingBuilder)
//    let clusterSettings = ClusterSettingsBuilder.op_Implicit(clusterBuilder)
//    let conn = EventStoreConnection.Create(settings, clusterSettings)
    let conn = EventStoreConnection.Create(settings, endpoint)
    conn.ApiWrap()

  let useCustomLogger logger (b : ConnectionSettingsBuilder) =
    b.UseCustomLogger logger

  let useConsoleLogger (b : ConnectionSettingsBuilder) =
    b.UseConsoleLogger ()

  let useDebugLogger (b : ConnectionSettingsBuilder) =
    b.UseDebugLogger ()

  let enableVerboseLogging logger (b : ConnectionSettingsBuilder) =
    b.EnableVerboseLogging ()
  
  let limitOperationsQueueTo (len : uint32) (b : ConnectionSettingsBuilder) =
    b.LimitOperationsQueueTo (int len)

  let limitAttemptsForOperationTo (len : uint32) (b : ConnectionSettingsBuilder) =
    b.LimitConcurrentOperationsTo (int len)

  let limitRetriesForOperationTo (len : uint32) (b : ConnectionSettingsBuilder) =
    b.LimitRetriesForOperationTo (int len)

  let keepRetrying (b : ConnectionSettingsBuilder) =
    b.KeepRetrying ()

  let limitReconnectionsTo limit (b : ConnectionSettingsBuilder) =
    b.LimitReconnectionsTo limit

  let keepReconnecting (b : ConnectionSettingsBuilder) =
    b.KeepReconnecting ()

  let performOnMasterOnly (b : ConnectionSettingsBuilder) =
    b.PerformOnMasterOnly ()

  let performOnAnyNode (b : ConnectionSettingsBuilder) =
    b.PerformOnAnyNode ()

  let setReconnectionDelayTo delay (b : ConnectionSettingsBuilder) =
    b.SetReconnectionDelayTo delay

  let setOperationTimeoutTo timeout (b : ConnectionSettingsBuilder) =
    b.SetOperationTimeoutTo timeout

  let setTimeoutCheckPeriodTo timeout (b : ConnectionSettingsBuilder) =
    b.SetTimeoutCheckPeriodTo timeout

  let setDefaultUserCredentials creds (b : ConnectionSettingsBuilder) =
    b.SetDefaultUserCredentials creds

  let useSslConnection targetHost validateServer (b : ConnectionSettingsBuilder) =
    b.UseSslConnection (targetHost, validateServer)

  let failOnNoServerResponse (b : ConnectionSettingsBuilder) =
    b.FailOnNoServerResponse ()

  let setHeartbeatInterval interval (b : ConnectionSettingsBuilder) =
    b.SetHeartbeatInterval interval

  let setHeartbeatTimeout timeout (b : ConnectionSettingsBuilder) =
    b.SetHeartbeatTimeout timeout

  let withConnectionTimeoutOf timeout (b : ConnectionSettingsBuilder) =
    b.WithConnectionTimeoutOf timeout

/// Module for dealing with EventStore transactions
module Tx =

  open Helpers
  open AsyncHelpers
  open EventStore.ClientAPI
  open Conn

  /// CommitAsync - commit the transaction
  let commit (tx : EventStoreTransaction) =
    tx.CommitAsync()
    |> awaitTask

  /// Rollback - rollback the transaction
  let rollback (tx : EventStoreTransaction) =
    tx.Rollback()

  /// get_TransactionId -
  /// Gets the unique identifier for the transaction.
  let id (tx : EventStoreTransaction) =
    tx.TransactionId

  /// WriteAsync
  let writeRaw (tx : EventStoreTransaction) (events : EventStore.ClientAPI.EventData list) =
    tx.WriteAsync(events)
    |> awaitTask

  /// WriteAsync
  let write (tx : EventStoreTransaction) (events : Types.EventData list) =
    writeRaw tx (events |> List.map unwrapEventData)


type ProjectionsCtx =
  { logger  : ILogger
    ep      : IPEndPoint
    timeout : TimeSpan
    creds   : SystemData.UserCredentials }

module Projections =

  open Chiron
  open Chiron.Operators

  open Helpers
  open AsyncHelpers

  open EventStore.ClientAPI
  open EventStore.ClientAPI.Projections
  open EventStore.ClientAPI.Exceptions

  [<Literal>]
  let Description = "A module that encapsulates read patterns against the event store"

  let private ``404 is just fine`` f =
    async {
      try
        return! f ()
      with 
      | :? ProjectionCommandFailedException as e
          when e.Message.Contains("404 (Not Found)") ->
        return ()
    }

  let private mkManager ctx =
    ProjectionsManager(ctx.logger, ctx.ep, ctx.timeout)

  let rec private handleHttpCodes (logger : ILogger) v =
    async {
      try
        let! status = v
        return Some status
      with e -> return! onHttpError logger v e e
    }
  and onHttpError logger v orig_ex (ex : exn) =
    async {
      match ex with
      | :? ProjectionCommandFailedException as pcfe
          when pcfe.Message.Contains "404 (Not Found)" ->
        return None
      | :? AggregateException as ae ->
        return! onHttpError logger v orig_ex (ae.InnerException)
      | :? WebException as we when we.Message.Contains "Aborted" ->
        return! handleHttpCodes logger v
      | e ->
        logger.Error (sprintf "unhandled exception from handle_http_codes:\n%O" e)
        return raise (Exception("unhandled exception from handle_http_codes", e))
    }

  let abort ctx name =
    let pm = mkManager ctx
    pm.AbortAsync(name, ctx.creds) |> Async.AwaitTask

  let enable ctx name =
    let pm = mkManager ctx
    pm.EnableAsync(name, ctx.creds) |> Async.AwaitTask

  let create_continuous ctx name query =
    let pm = mkManager ctx
    pm.CreateContinuousAsync(name, query, ctx.creds) |> Async.AwaitTask

  let create_one_time ctx query =
    let pm = mkManager ctx
    pm.CreateOneTimeAsync(query, ctx.creds) |> Async.AwaitTask

  let create_transient ctx name query =
    let pm = mkManager ctx
    pm.CreateTransientAsync(name, query, ctx.creds) |> Async.AwaitTask

  let delete ctx name =
    let pm = mkManager ctx
    pm.DeleteAsync(name, ctx.creds)
    |> Async.AwaitTask
    |> handleHttpCodes ctx.logger
    |> Async.Ignore

  let disable ctx name =
    let pm = mkManager ctx
    ``404 is just fine`` <| fun _ ->
      pm.DisableAsync(name, ctx.creds)
      |> Async.AwaitTask

  let get_query ctx name =
    let pm = mkManager ctx
    pm.GetQueryAsync(name, ctx.creds) |> Async.AwaitTask

  let getState ctx name =
    let pm = mkManager ctx
    pm.GetStateAsync(name, ctx.creds)
    |> Async.AwaitTask
    |> handleHttpCodes ctx.logger

  let getStatistics ctx name =
    let pm = mkManager ctx
    pm.GetStatisticsAsync(name, ctx.creds) |> Async.AwaitTask

  // Booh yah!
  type Status =
    { coreProcessingTime : uint32
      version : uint32
      epoch : int
      effectiveName : string
      writesInProgress : uint16
      readsInProgress  : uint16
      partitionsCached : uint16
      status : string
      stateReason : string
      name : string
      mode : string
      position : string
      progress : float
      lastCheckpoint : string
      eventsProcessedAfterRestart : uint32
      statusUrl : Uri
      stateUrl : Uri
      resultUrl : Uri
      queryUrl : Uri
      enableCommandUrl : Uri
      disableCommandUrl : Uri
      checkpointStatus : string
      bufferedEvents : uint32
      writePendingEventsBeforeCheckpoint : uint32
      writePendingEventsAfterCheckpoint : uint32
    }
    static member FromJson (_ : Status) =
      (fun cpt ver ep effName wip rip pc st sReas nm mo pos prog lc epar statU stateU resU qU ecU dcU cStat bE wpebc wpeac ->
        { coreProcessingTime = cpt
          version = ver
          epoch = ep
          effectiveName = effName
          writesInProgress = wip
          readsInProgress  = rip
          partitionsCached = pc
          status = st
          stateReason = sReas
          name = nm
          mode = mo
          position = pos
          progress = prog
          lastCheckpoint = lc
          eventsProcessedAfterRestart = epar
          statusUrl = Uri statU
          stateUrl = Uri stateU
          resultUrl = Uri resU
          queryUrl = Uri qU
          enableCommandUrl = Uri ecU
          disableCommandUrl = Uri dcU
          checkpointStatus = cStat
          bufferedEvents = bE
          writePendingEventsBeforeCheckpoint = wpebc
          writePendingEventsAfterCheckpoint = wpeac })
      <!> Json.read "coreProcessingTime"
      <*> Json.read "version"
      <*> Json.read "epoch"
      <*> Json.read "effectiveName"
      <*> Json.read "writesInProgress"
      <*> Json.read "readsInProgress"
      <*> Json.read "partitionsCached"
      <*> Json.read "status"
      <*> Json.read "stateReason"
      <*> Json.read "name"
      <*> Json.read "mode"
      <*> Json.read "position"
      <*> Json.read "progress"
      <*> Json.read "lastCheckpoint"
      <*> Json.read "eventsProcessedAfterRestart"
      <*> Json.read "statusUrl"
      <*> Json.read "stateUrl"
      <*> Json.read "resultUrl"
      <*> Json.read "queryUrl"
      <*> Json.read "enableCommandUrl"
      <*> Json.read "disableCommandUrl"
      <*> Json.read "checkpointStatus"
      <*> Json.read "bufferedEvents"
      <*> Json.read "writePendingEventsBeforeCheckpoint"
      <*> Json.read "writePendingEventsAfterCheckpoint"

    static member ToJson (s : Status) =
      Json.write "coreProcessingTime" s.coreProcessingTime
      *> Json.write "version" s.version
      *> Json.write "epoch" s.epoch
      *> Json.write "effectiveName" s.effectiveName
      *> Json.write "writesInProgress" s.writesInProgress
      *> Json.write "readsInProgress" s.readsInProgress
      *> Json.write "partitionsCached" s.partitionsCached
      *> Json.write "status" s.status
      *> Json.write "stateReason" s.stateReason
      *> Json.write "name" s.name
      *> Json.write "mode" s.mode
      *> Json.write "position" s.position
      *> Json.write "progress" s.progress
      *> Json.write "lastCheckpoint" s.lastCheckpoint
      *> Json.write "eventsProcessedAfterRestart" s.eventsProcessedAfterRestart
      *> Json.write "statusUrl" (s.statusUrl.ToString())
      *> Json.write "stateUrl" (s.stateUrl.ToString())
      *> Json.write "resultUrl" (s.resultUrl.ToString())
      *> Json.write "queryUrl" (s.queryUrl.ToString())
      *> Json.write "enableCommandUrl" (s.enableCommandUrl.ToString())
      *> Json.write "disableCommandUrl" (s.disableCommandUrl.ToString())
      *> Json.write "checkpointStatus" s.checkpointStatus
      *> Json.write "bufferedEvents" s.bufferedEvents
      *> Json.write "writePendingEventsBeforeCheckpoint" s.writePendingEventsBeforeCheckpoint
      *> Json.write "writePendingEventsAfterCheckpoint" s.writePendingEventsAfterCheckpoint

  let getStatus ctx name =
    let pm = mkManager ctx
    async {
      let! res =
        pm.GetStatusAsync(name, ctx.creds)
        |> Async.AwaitTask
        |> handleHttpCodes ctx.logger
      try
        return res |> Option.map (Json.parse >> (Json.deserialize : _ -> Status))
      with
      | :? System.OverflowException as e ->
        return failwithf "res: %s" (if res.IsSome then res.Value else "-")
    }

  let listAll ctx =
    let pm = mkManager ctx
    pm.ListAllAsync ctx.creds |> Async.AwaitTask

  let listContinuous ctx =
    let pm = mkManager ctx
    pm.ListContinuousAsync ctx.creds |> Async.AwaitTask

  let listOneTime ctx =
    let pm = mkManager ctx
    pm.ListOneTimeAsync ctx.creds |> Async.AwaitTask

  let updateQuery ctx name query =
    let pm = mkManager ctx
    pm.UpdateQueryAsync(name, query, ctx.creds) |> Async.AwaitTask

  let setupAggregateProjections ctx  =
    let streams = [ "$by_category"; "$stream_by_category" ]
    async {
      for stream in streams do
        let! status = stream |> getStatus ctx
        match status with
        | None ->
          do! stream |> enable ctx
        | Some status when status.status <> "Running" ->
          ctx.logger.Debug (sprintf "projection '%s' in status '%A', enabling now"  stream status)
          do! stream |> enable ctx
        | _ -> return ()
    }

  let rec waitForInit ctx (f : unit -> Async<_>) = async {
    let pm = mkManager ctx
    try
      do! setupAggregateProjections ctx
      return! f ()
    with e -> return! onError ctx f e e
    }
  and onError ctx f origErr err = async {
    match err with
    | :? AggregateException as ae -> // unwrap it
      return! onError ctx f origErr (ae.InnerException)
    | :? ProjectionCommandFailedException as e
        when e.Message.Contains("Not yet ready.") ->
      ctx.logger.Info "... waiting 1000 ms for server to get ready ..."
      do! Async.Sleep 1000
      return! waitForInit ctx f
    | e when e.InnerException <> null ->
      return! onError ctx f origErr (e.InnerException)
    | e ->
      raise (Exception("exception in waitForInit", origErr))
    }

  let ensureContinuous ctx name query =
    let pm = mkManager ctx
    async {
      ctx.logger.Debug "calling get_status"
      let! status = getStatus ctx name
      match status with
      | None -> 
        ctx.logger.Debug "calling ensureContinuous from 404 case"
        do! create_continuous ctx name query
      | Some status when status.status <> "Running" ->
        ctx.logger.Debug "calling ensureContinuous from not Running case"
        do! create_continuous ctx name query
      | other ->
        ctx.logger.Debug (sprintf "got %O back" other)
        return ()
    }

/// A connection wrapper that ensures the connection is connected when it's being
/// used.
type ConnectionWrapper(esLogger, tcpEndpoint) =
  let mutable connected = false

  let connection =
    ConnectionSettings.configureStart ()
    |> ConnectionSettings.useCustomLogger esLogger
    |> ConnectionSettings.keepReconnecting
    |> ConnectionSettings.configureEnd tcpEndpoint

  let ensureConnected () = async {
    if not connected then
      do! Conn.connect connection
      connected <- true
    }

  member x.Execute<'a> (f : Connection -> Async<'a>) = async {
    do! ensureConnected ()
    return! f connection
  }

  member x.Connection =
    ensureConnected () |> Async.RunSynchronously
    connection

  interface IDisposable with
    member x.Dispose() =
      Conn.close connection
