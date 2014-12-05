namespace EventStore.ClientAPI

// https://github.com/EventStore/EventStore/wiki/Writing-Events-%28.NET-API%29

/// The specific event version to read as detailed on ReadEventAsync
type EventVersion =
  | Specific of uint32
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

module internal Helpers =

  let validUint (b : uint32) =
    if b > uint32(System.Int32.MaxValue) then
      failwith <| sprintf "too large start %A, gt int32 max val" b
    else int b

  let action  f = new System.Action<_>(f)
  let action2 f = new System.Action<_, _>(f)
  let action3 f = new System.Action<_, _, _>(f)

  open EventStore.ClientAPI

  let (|EventVersion|) = function
    | EventVersion.Specific i -> int i
    | LastInStream            -> -1

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
    //   : (('a -> 'b) -> ('c -> Async<'a>) -> 'c -> Async<'b>)
    let MapCompose fMap fOrig c =
      async {
        let! res = fOrig c
        return fMap res }

    let (<<!) = MapCompose

// https://github.com/eulerfx/DDDInventoryItemFSharp/blob/master/DDDInventoryItemFSharp/EventStore.fs
// https://github.com/EventStore/getting-started-with-event-store/blob/master/src/GetEventStoreRepository/GetEventStoreRepository.cs
// https://github.com/EventStore/getting-started-with-event-store/blob/master/src/GetEventStoreRepository.Tests/GetEventStoreRepositoryIntegrationTests.cs

/// Wrapper F# API for the connection settings builder
/// in the event store client API.
module ConnBuilder =

  open Helpers
  open EventStore.ClientAPI

  let enableVerboseLogging (s : ConnectionSettingsBuilder) =
    s.EnableVerboseLogging ()

  let keepReconnecting (s : ConnectionSettingsBuilder) =
    s.KeepReconnecting ()

  let keepRetrying (s : ConnectionSettingsBuilder) =
    s.KeepRetrying ()

  let limitAttemptsForOperationTo limit (s : ConnectionSettingsBuilder) =
    s.LimitAttemptsForOperationTo limit

  let limitConcurrentOperationsTo limit (s : ConnectionSettingsBuilder) =
    s.LimitConcurrentOperationsTo limit

  let limitOperationsQueueTo limit (s : ConnectionSettingsBuilder) =
    s.LimitOperationsQueueTo limit

  let limitReconnectionsTo limit (s : ConnectionSettingsBuilder) =
    s.LimitReconnectionsTo limit

  let limitRetriesForOperationTo limit (s : ConnectionSettingsBuilder) =
    s.LimitRetriesForOperationTo limit

  let setOperationTimeout timeout (s : ConnectionSettingsBuilder) =
    s.SetOperationTimeoutTo timeout

open System
open System.Net
open System.Threading.Tasks

type Count = uint32
type Offset = uint32
type StreamId = string
type TransactionId = int64

/// Wrapper interface for <see cref="EventStore.ClientAPI.IEventStoreConnection" />
/// that has all the important operations.
/// Consumable from C# in order to allow easy mocking and stubbing.
/// See the 'Conn' module for documentation of each method.
type Connection =
  inherit IDisposable

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L181
  abstract AppendToStreamAsync           : StreamId
                                            * ExpectedVersionUnion
                                            * SystemData.UserCredentials option
                                            * EventData seq
                                            -> WriteResult Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L69
  abstract Close                         : unit -> unit

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L64
  abstract ConnectAsync                  : unit -> Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L53
  abstract ConnectionName                : unit -> string with get

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L242
  abstract ContinueTransaction           : TransactionId
                                            * SystemData.UserCredentials option
                                            -> EventStoreTransaction

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L86
  abstract DeleteStreamAsync             : StreamId
                                            * ExpectedVersionUnion
                                            * SystemData.UserCredentials option
                                            -> DeleteResult Task

  // all read
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L346
  abstract ReadAllEventsBackwardAsync    : Position * Count * ResolveLinksStrategy
                                            * SystemData.UserCredentials option
                                            -> AllEventsSlice Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L326
  abstract ReadAllEventsForwardAsync     : Position * Count * ResolveLinksStrategy
                                            * SystemData.UserCredentials option
                                            -> AllEventsSlice Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L262
  abstract ReadEventAsync                : StreamId
                                            * EventVersion
                                            * ResolveLinksStrategy
                                            * SystemData.UserCredentials option
                                            -> EventReadResult Task

  // stream read
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L306
  abstract ReadStreamEventsBackwardAsync : StreamId * Offset * Count
                                            * ResolveLinksStrategy
                                            * SystemData.UserCredentials option
                                            -> StreamEventsSlice Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L284
  abstract ReadStreamEventsForwardAsync  : StreamId * Offset * Count
                                            * ResolveLinksStrategy
                                            * SystemData.UserCredentials option
                                            -> StreamEventsSlice Task

  // metadata, skipping 'raw' methods so far
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L393
  abstract SetStreamMetadataAsync        : StreamId
                                            * ExpectedVersionUnion
                                            * StreamMetadata
                                            * SystemData.UserCredentials option
                                            -> WriteResult Task

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L401
  abstract GetStreamMetadataAsync        : StreamId
                                            * SystemData.UserCredentials option
                                            -> StreamMetadataResult Task

  // tx
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L229
  abstract StartTransactionAsync         : StreamId
                                            * ExpectedVersionUnion
                                            * SystemData.UserCredentials option
                                           -> EventStoreTransaction Task

  // subscribe API
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L355
  abstract SubscribeToStreamAsync        : StreamId
                                            // resolveLinks:
                                            * ResolveLinksStrategy
                                            // eventAppeared:
                                            * Action<EventStoreSubscription, ResolvedEvent>
                                            // subscriptionDropped:
                                            * Action<EventStoreSubscription, SubscriptionDropReason, exn> option
                                            * SystemData.UserCredentials option
                                            -> EventStoreSubscription Task

  /// Subscribe from, offset exclusive
  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L362
  abstract SubscribeToStreamFrom         : StreamId
                                            * Offset option
                                            // resolveLinks:
                                            * ResolveLinksStrategy
                                            // eventAppeared:
                                            * Action<EventStoreCatchUpSubscription, ResolvedEvent>
                                            // liveProcessingStarted:
                                            * Action<EventStoreCatchUpSubscription> option
                                            // subscriptionDropped:
                                            * Action<EventStoreCatchUpSubscription, SubscriptionDropReason, exn> option
                                            * SystemData.UserCredentials option
                                            -> EventStoreStreamCatchUpSubscription

  /// https://github.com/EventStore/EventStore/blob/ES-NET-v2.0.1/src/EventStore/EventStore.ClientAPI/IEventStoreConnection.cs#L377
  abstract SubscribeToAllAsync           : ResolveLinksStrategy
                                            // eventAppeared:
                                            * Action<EventStoreSubscription, ResolvedEvent>
                                            // subscriptionDropped:
                                            * Action<EventStoreSubscription, SubscriptionDropReason, exn> option
                                            * SystemData.UserCredentials option
                                            -> Task<EventStoreSubscription>

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
                                            * SystemData.UserCredentials option
                                            -> EventStoreAllCatchUpSubscription

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
          member x.AppendToStreamAsync(streamId, expectedVersion, userCredentials, events) =
            match expectedVersion with
            | ExpectedVersion expectedVersion ->
              y.AppendToStreamAsync(streamId, expectedVersion, events, Option.noneIsNull userCredentials)

          member x.ConnectionName =
            y.ConnectionName

          member x.ConnectAsync () =
            y.ConnectAsync ()

          member x.Close () =
            y.Close()

          member x.DeleteStreamAsync (streamId, expectedVersion, userCredentials) =
            match expectedVersion with
            | ExpectedVersion expectedVersion ->
              y.DeleteStreamAsync(streamId, expectedVersion, Option.noneIsNull userCredentials)

          member x.ReadAllEventsForwardAsync (position, maxCount, resolveLinks, userCredentials) =
            let maxCount = validUint maxCount
            y.ReadAllEventsForwardAsync(position, maxCount, 
                                        (match resolveLinks with ResolveLinks r -> r),
                                        Option.noneIsNull userCredentials)
          member x.ReadAllEventsBackwardAsync (position, maxCount, resolveLinks, userCredentials) =
            let maxCount = validUint maxCount
            y.ReadAllEventsBackwardAsync(position, maxCount,
                                         (match resolveLinks with ResolveLinks r -> r),
                                         Option.noneIsNull userCredentials)

          member x.ReadEventAsync(stream, eventVersion, resolveLinks, userCredentials) =
            match eventVersion with
            | EventVersion eventVersion ->
              y.ReadEventAsync(stream, eventVersion,
                               (match resolveLinks with ResolveLinks r -> r),
                               Option.noneIsNull userCredentials)

          member x.ReadStreamEventsBackwardAsync (streamId, start, count, resolveLinks, userCredentials) =
            let start, count = validUint start, validUint count
            y.ReadStreamEventsBackwardAsync(streamId, start, count,
                                            (match resolveLinks with ResolveLinks r -> r),
                                            Option.noneIsNull userCredentials)

          member x.ReadStreamEventsForwardAsync (streamId, start, count, resolveLinks, userCredentials) =
            let start, count = validUint start, validUint count
            y.ReadStreamEventsForwardAsync(streamId, start, count,
                                           (match resolveLinks with ResolveLinks r -> r),
                                           Option.noneIsNull userCredentials)

          member x.SetStreamMetadataAsync (streamId, expectedVersion, metadata, userCredentials) =
            match expectedVersion with
            | ExpectedVersion expectedVersion ->
              y.SetStreamMetadataAsync(streamId, expectedVersion, metadata, Option.noneIsNull userCredentials)

          member x.GetStreamMetadataAsync (streamId, userCredentials) =
            y.GetStreamMetadataAsync(streamId, Option.noneIsNull userCredentials)

          member x.StartTransactionAsync (streamId, expectedVersion, userCredentials) =
            match expectedVersion with
            | ExpectedVersion expectedVersion ->
              y.StartTransactionAsync(streamId, expectedVersion, Option.noneIsNull userCredentials)
              
          member x.ContinueTransaction(txId, userCredentials) =
            y.ContinueTransaction(txId, Option.noneIsNull userCredentials)

          member x.SubscribeToStreamAsync (streamId, resolveLinks, eventAppeared, subscriptionDropped, userCredentials) =
            let resolveLinks = match resolveLinks with ResolveLinks r -> r
            y.SubscribeToStreamAsync(streamId, resolveLinks, eventAppeared,
                                     Option.noneIsNull subscriptionDropped,
                                     Option.noneIsNull userCredentials)

          member x.SubscribeToStreamFrom (streamId, fromEventNoExclusive, resolveLinks,
                                          eventAppeared, liveProcessingStarted,
                                          subscriptionDropped, userCredentials) =
            let fromEventNoExclusive = Option.bind (validUint >> Some) fromEventNoExclusive
            let resolveLinks = match resolveLinks with ResolveLinks r -> r
            y.SubscribeToStreamFrom(streamId,
                                    Option.toNullable fromEventNoExclusive,
                                    resolveLinks, eventAppeared,
                                    Option.noneIsNull liveProcessingStarted,
                                    Option.noneIsNull subscriptionDropped,
                                    Option.noneIsNull userCredentials)

          member x.SubscribeToAllAsync (resolveLinks, eventAppeared, subscriptionDropped,
                                        userCredentials) =
            let resolveLinks = match resolveLinks with ResolveLinks r -> r
            y.SubscribeToAllAsync(resolveLinks, eventAppeared,
                                  Option.noneIsNull subscriptionDropped,
                                  Option.noneIsNull userCredentials)
          member x.SubscribeToAllFrom (fromPos, resolveLinks, eventAppeared,
                                       liveProcessingStarted, subscriptionDropped, userCredentials) = 
            let resolveLinks = match resolveLinks with ResolveLinks r -> r
            y.SubscribeToAllFrom(Option.toNullable fromPos,
                                 resolveLinks, eventAppeared,
                                 Option.noneIsNull liveProcessingStarted,
                                 Option.noneIsNull subscriptionDropped,
                                 Option.noneIsNull userCredentials)
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

  let private newRecordedEvent (eventId : Guid) (streamId : string) (evtType : string) (number : int) (data : byte array) (metaData : byte array) =
    ctorRecordedEvent ()
    |> setterRecE "EventStreamId" streamId
    |> setterRecE "EventId" eventId
    |> setterRecE "EventNumber" number
    |> setterRecE "EventType" evtType
    |> setterRecE "Data" data
    |> setterRecE "Metadata" metaData

  let private emptyRecordedEvent =
    newRecordedEvent Guid.Empty "" "" 0 Empty.ByteArray Empty.ByteArray

  type EventStore.ClientAPI.RecordedEvent with
    /// Gets an empty recorded event, for interop purposes.
    static member Empty = emptyRecordedEvent

  type RecordedEvent =
    { Id       : System.Guid
      StreamId : string
      Type     : string
      Metadata : byte array
      Data     : byte array
      Number   : uint32 }
    static member FromEventData streamId number (evt : EventData) =
      { Id       = evt.Id
        StreamId = streamId
        Type     = evt.Type
        Metadata = evt.Metadata
        Data     = evt.Data
        Number   = number }

  let unwrapRecordedEvent (e : RecordedEvent) =
    let number = validUint e.Number
    newRecordedEvent e.Id e.StreamId e.Type number e.Data e.Metadata

  let wrapRecordedEvent (e : EventStore.ClientAPI.RecordedEvent) =
    if obj.ReferenceEquals(e, null) then invalidArg "e" "e is null"
    { Id       = e.EventId
      StreamId = e.EventStreamId
      Type     = e.EventType
      Metadata = e.Metadata
      Data     = e.Data
      Number   = e.EventNumber |> uint32 }

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
    { Event               : RecordedEvent
      Link                : RecordedEvent option
      OriginalPosition    : Position option }

  let unwrapResolvedEvent (e : ResolvedEvent) =
    newResolvedEvent (e.Event |> unwrapRecordedEvent) (e.Link |> Option.fold (fun s -> unwrapRecordedEvent) null) (Option.toNullable e.OriginalPosition)

  /// Convert a <see cref="EventStore.ClientAPI.ResolvedEveRent" /> to a
  /// DataModel.EventRef.
  let wrapResolvedEvent (e : EventStore.ClientAPI.ResolvedEvent) =
    { Event            = e.Event |> wrapRecordedEvent
      Link             = None // e.Link  |> Option.foldObj (fun s -> wrapRecordedEvent) None
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
    (events : EventStore.ClientAPI.ResolvedEvent array) =
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
    ; FromPosition  : Position
    ; NextPosition  : Position
    ; ReadDirection : ReadDirection }

  type AllEventsSlice with
    static member Empty = { Events = []; FromPosition = Position(); NextPosition = Position(); ReadDirection = Forward }

  let unwrapAllEventsSlice (e : AllEventsSlice) =
    newAES (unwrapReadDirection e.ReadDirection)
      e.FromPosition
      e.NextPosition
      (e.Events |> List.map unwrapResolvedEvent |> List.toArray)

  let wrapAllEventsSlice (e : EventStore.ClientAPI.AllEventsSlice) =
    { Events = e.Events |> List.ofArray |> List.map wrapResolvedEvent
      FromPosition = e.FromPosition
      NextPosition = e.FromPosition
      ReadDirection = e.ReadDirection |> wrapReadDirection }

/// Thin F# wrapper on top of EventStore.Client's API. Doesn't
/// translate any structures, but enables currying and F# calling conventions.
/// The EventStoreConnection class is responsible for maintaining a full-duplex connection between the client and the event store server. EventStoreConnection is thread-safe, and it is recommended that only one instance per application is created.
/// This module has its methods sorted by the 'Connection' interface.
module Conn =

  open Helpers
  open AsyncHelpers

  // BUILDING CONNECTION

  /// Create a builder and use the module ConnBuilder to set the settings.
  /// Call 'configureEnd' when you are done, to get the connection.
  let configureStart () =
    ConnectionSettings.Create()

  /// End configuring the connection settings and return
  /// a new connection with those settings (not connected).
  let configureEnd (endpoint : IPEndPoint) (settingBuilder : ConnectionSettingsBuilder) = //, clusterBuilder : ClusterSettingsBuilder) =
    let settings = ConnectionSettingsBuilder.op_Implicit(settingBuilder)
//    let clusterSettings = ClusterSettingsBuilder.op_Implicit(clusterBuilder)
//    let conn = EventStoreConnection.Create(settings, clusterSettings)
    let conn = EventStoreConnection.Create(settings, endpoint)
    conn.ApiWrap()

  // CONNECTION API

  /// AppendToStreamAsync:
  /// see docs for append.
  let internal appendRaw (c : Connection) streamId expectedVersion credentials (eventData : EventStore.ClientAPI.EventData list) =
    c.AppendToStreamAsync(streamId, expectedVersion, credentials, eventData)
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
  let append (c : Connection) credentials streamId expectedVersion (eventData : EventData list) =
    async { return! appendRaw c streamId expectedVersion credentials (eventData |> List.map unwrapEventData) }

  /// Close
  let close (c : Connection) =
    c.Close()

  /// ConnectAsync
  let connect (c : Connection) =
    if obj.ReferenceEquals(null, c) then invalidArg "c" "c is null"
    c.ConnectAsync() |> awaitTask

  let name (c : Connection) = c.ConnectionName

  /// ContinueTransaction
  let continueTransaction (c : Connection) txId =
    c.ContinueTransaction txId

  /// DeleteStreamAsync
  let delete (c : Connection) expectedVersion streamId userCredentials =
    c.DeleteStreamAsync(streamId, expectedVersion, userCredentials)
    |> awaitTask
    |> canThrowWrongExpectedVersion

  // TODO: Types.AllEventSlice
  /// ReadAllEventsBackwardAsync
  let readAllBack position maxCount resolveLinks userCredentials (c : Connection) =
    c.ReadAllEventsBackwardAsync(position, maxCount, resolveLinks, userCredentials)
      |> Async.AwaitTask

  // TODO: Types.AllEventSlice
  /// ReadAllEventsForwardAsync
  let readAll position maxCount resolveLinks userCredentials (c : Connection) =
    c.ReadAllEventsForwardAsync(position, maxCount, resolveLinks, userCredentials)
      |> Async.AwaitTask

  /// ReadEventAsync
  let internal readEventRaw stream expectedVersion resolveLinks userCredentials (c : Connection) =
    c.ReadEventAsync(stream, expectedVersion, resolveLinks, userCredentials)
      |> Async.AwaitTask

  /// ReadEventAsync
  let readEvent stream expectedVersion resolveLinks userCredentials (c : Connection) =
    async { let! res = c |> readEventRaw stream expectedVersion resolveLinks userCredentials
            return res |> wrapEventReadResult }

  /// ReadStreamEventsBackwardAsync
  let internal readBackRaw streamId start count resolveLinks userCredentials (c : Connection) =
    c.ReadStreamEventsBackwardAsync(streamId, start, count, resolveLinks, userCredentials)
      |> Async.AwaitTask

  /// ReadStreamEventsBackwardAsync
  let readBack streamId start count resolveLinks userCredentials (c : Connection) =
    async { let! res = c |> readBackRaw streamId start count resolveLinks userCredentials
            return res |> wrapStreamEventsSlice }

  /// ReadStreamEventsForwardAsync
  /// <param name="start">Where to start reading, inclusive</param>
  let internal readRaw streamId start count resolveLinkTos userCredentials (c : Connection) =
    c.ReadStreamEventsForwardAsync(streamId, start, count, resolveLinkTos, userCredentials)
      |> Async.AwaitTask

  /// ReadStreamEventsForwardAsync
  /// <param name="start">Where to start reading, inclusive</param>
  let read streamId start count resolveLinkTos userCredentials (c : Connection) =
    async { let! res = c |> readRaw streamId start count resolveLinkTos userCredentials
            return res |> wrapStreamEventsSlice }

  /// SetStreamMetadataAsync
  let setMetadata streamId expectedVersion metadata userCredentials (c : Connection) =
    c.SetStreamMetadataAsync(streamId, expectedVersion, metadata, userCredentials)
      |> awaitTask

  /// GetStreamMetadataAsync
  let getMetadata streamId userCredentials (c : Connection) =
    c.GetStreamMetadataAsync(streamId, userCredentials)
      |> Async.AwaitTask

  /// StartTransactionAsync
  let startTx streamId expectedVersion userCredentials (c : Connection) =
    c.StartTransactionAsync(streamId, expectedVersion, userCredentials) 
      |> Async.AwaitTask
      |> canThrowWrongExpectedVersion

  /// SubscribeToStream
  let subscribe (c : Connection) streamId resolveLinks eventAppeared subscriptionDropped userCredentials =
    c.SubscribeToStreamAsync(streamId, resolveLinks,
                             eventAppeared |> action2,
                             subscriptionDropped |> Option.map action3,
                             userCredentials)

  /// SubscribeToStreamFrom
  let subscribeFrom (c : Connection) streamId fromEvt resolveLinks eventAppeared
    liveProcessingStarted subscriptionDropped userCredentials =
    c.SubscribeToStreamFrom(streamId, fromEvt, resolveLinks, eventAppeared |> action2,
                            liveProcessingStarted |> Option.map action,
                            subscriptionDropped |> Option.map action3,
                            userCredentials)

  /// SubscribeToAll
  let subscribeAll (c : Connection) resolveLinks eventAppeared subscriptionDropped userCredentials =
    c.SubscribeToAllAsync(resolveLinks, eventAppeared |> action2,
                          subscriptionDropped |> Option.map action3,
                          userCredentials)
      |> Async.AwaitTask

  /// SubscribeToAllFrom
  let subscribeAllFrom (c : Connection) fromPos resolveLinks eventAppeared 
    liveProcessingStarted subscriptionDropped userCredentials =
    c.SubscribeToAllFrom(fromPos, resolveLinks, eventAppeared |> action2,
                         liveProcessingStarted |> Option.map action,
                         subscriptionDropped |> Option.map action3,
                         userCredentials)

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

module Events =
  let [<Literal>] EventTypeKey = "EventClrTypeName"

  open System.Text

  type Type with
    /// Convert the type to a partially qualified name that also contains
    /// the types of the type parameters (if the type is generic).
    /// Throws argument exception if the type is an open generic type.
    member t.ToPartiallyQualifiedName () =
      if t.IsGenericTypeDefinition then invalidArg "open generic types are not allwed" "t"
      let sb = new StringBuilder()
      let append = (fun s -> sb.Append(s) |> ignore) : string -> unit

      append t.FullName

      if t.IsGenericType then
        append "["
        let args = t.GetGenericArguments() |> Array.map (fun g -> sprintf "[%s]" <| g.ToPartiallyQualifiedName())
        append <| String.Join(", ", args)
        append "]"

      append ", "
      append <| t.Assembly.GetName().Name

      sb.ToString()

  open System
  open System.Net
  open System.Security.Cryptography
  open System.Text

  open EventStore.ClientAPI

  let to_json = Newtonsoft.Json.JsonConvert.SerializeObject
  let to_jsonb : obj -> byte[] = (Encoding.UTF8.GetBytes : string -> byte[]) << Newtonsoft.Json.JsonConvert.SerializeObject

  let to_guid (s : string) : Guid =
    use sha = HashAlgorithm.Create "SHA1"
    let sha1 = sha.ComputeHash : byte[] -> byte[]
    let guid = fun (bs : byte []) -> Guid bs
    let truncate bs =
      let bs' = Array.zeroCreate<byte> 16
      Array.blit bs 0 bs' 0 16
      bs'
    s |> (Encoding.UTF8.GetBytes >> sha1 >> truncate >> guid)

  type EventData with
    /// Create an EventData from an object and given a natural event name
    /// that is used in the projections
    static member From<'a> (item : 'a) naturalEventName =
      let json = to_json item
      { Id       = to_guid json
      ; Type     = naturalEventName
      ; Metadata = [ (EventTypeKey, typeof<'a>.ToPartiallyQualifiedName()) ]
                   |> Map.ofList |> to_jsonb
      ; Data     = Encoding.UTF8.GetBytes json
      ; IsJson   = true }

module Read =
  [<Literal>]
  let Description = "A module that encapsulates read patterns against the event store"

module Write =
  [<Literal>]
  let Description = "A module that encapsulates write patterns against the event store"

/// Aggregate framework.
[<RequireQualifiedAccess>]
module Aggregate =

  open FSharp.Control

  /// Represents an aggregate.
  type Aggregate<'TState, 'TCommand, 'TEvent> =
    { /// An initial state value.
      zero : 'TState
      /// Applies an event to a state returning a new state.
      apply : 'TState -> 'TEvent -> 'TState
      /// Executes a command on a state yielding one or many events. Does not yield
      /// all events that the aggregate is based on.
      exec : 'TState -> 'TCommand -> 'TEvent list }

  /// An aggregate id
  type Id = string

  /// The aggregate version, incremented by one for each commit.
  type Version = uint32

  let makeExecutor
    (aggregate : Aggregate<'TState, 'TCommand, 'TEvent>)
    (load : System.Type * Id * ExpectedVersionUnion -> AsyncSeq<'TEvent>) =
    fun (id, version : ExpectedVersionUnion) command -> async {
      let events = load (typeof<'TEvent>, id, version)
      let! state = events |> AsyncSeq.fold aggregate.apply aggregate.zero
      return aggregate.exec state command }

  /// Creates a persistent command handler for an aggregate.
  let makeHandler
    (aggregate : Aggregate<'TState, 'TCommand, 'TEvent>)
    (load      : System.Type * Id * ExpectedVersionUnion -> AsyncSeq<'TEvent>)
    (commit    : Id * ExpectedVersionUnion -> 'TEvent list -> Async<unit>) =
    let executor = makeExecutor aggregate load
    fun (id, version) command -> async {
      let! events = executor (id, version) command
      return events, events |> commit (id, version) }

/// F# list methods
module internal List =
  /// Split xs at n, into two lists, or where xs ends if xs.Length < n.
  let split n xs =
    let rec splitUtil n xs acc =
      match xs with
      | [] -> List.rev acc, []
      | _ when n = 0u -> List.rev acc, xs
      | x::xs' -> splitUtil (n-1u) xs' (x::acc)
    splitUtil n xs []

  /// Chunk a list into pageSize large chunks
  let chunk pageSize = function
    | [] -> None
    | l -> let h, t = l |> split pageSize in Some(h, t)

module Repo =

  open EventStore.ClientAPI
  open Conn
  open Write
  open Events

  open FSharp.Control

  open System

  type AsyncSeq.AsyncSeqBuilder with
    member x.ReturnFrom(s : AsyncSeq<'a>) =
      s

  let private formatId : obj -> string = function
    | :? Guid as g -> g.ToString "N"
    | :? string as s -> s
    | _ as other -> other.ToString ()

  let private streamId (id: Aggregate.Id) = formatId (box id)

  type StreamWrongExpectedVersion(inner : Exceptions.WrongExpectedVersionException) =
    inherit Exception("Repo concurrency check failed", inner)

  exception StreamNotFoundException of StreamId
  exception StreamHasBeenDeletedException of StreamId

  /// Header key with commit id; a unique one for every commit
  [<Literal>]
  let CommitIdKey = "CommitId"

  /// How many events each page fetched from the event store should be
  [<Literal>]
  let internal PageSize = 512u

  /// Load takes the aggregate type, the
  /// Prototype: https://github.com/EventStore/getting-started-with-event-store/blob/master/src/GetEventStoreRepository/GetEventStoreRepository.cs#L57
  let load (deserialise : Type * byte array -> obj)
           conn
           (t : Type, id : Aggregate.Id, maxVersion : ExpectedVersionUnion)
           : 'a AsyncSeq =

    let sliceCount sliceStart version =
      if sliceStart + PageSize <= version then PageSize
      else version - sliceStart + 1u

    let uintMaxVersion =
      match maxVersion with
      | Specific s -> s
      | _   -> Aggregate.Version.MaxValue

    let streamId = streamId id

    let readSlice = function
      | { Events          = refs
          Stream          = streamId
          FromEventNumber = fromNo
          ReadDirection   = dir
          NextEventNumber = nextNo
          LastEventNumber = lastNo } ->
        refs |> Seq.map (fun e -> deserialise (t, e.Event.Data) :?> 'a)

    let rec loadSlice start count = asyncSeq {
      let! ses = conn |> read streamId start count DontResolveLinks None
      match ses with
      | NotFound _ ->
        return! AsyncSeq.empty
      | Deleted _ ->
        raise <| StreamHasBeenDeletedException id
      | Success(StreamSlice slice) ->
        for evt in readSlice slice do yield (evt : 'a)
        return! loadSlice slice.NextEventNumber (sliceCount start uintMaxVersion)
      | Success(EndOfStream slice) ->
        for evt in readSlice slice do yield evt }
          // e.Event.EventType => the semantic event type (not the CLR type)

    loadSlice 0u PageSize

  let commit (serialise : obj -> _) conn (id, expected : ExpectedVersionUnion) (es : 'a list) =

    let commitHeaders = [ CommitIdKey, box(Guid.NewGuid ()) ] |> Map.ofList
    let streamId = streamId id

    let prepareData headers (e : 'a) =
      let headers' = headers |> Map.add EventTypeKey (typeof<'a>.ToPartiallyQualifiedName() |> box)
      let eventType, data = serialise e
      let _, metaData = serialise headers'
      { Id       = Guid.NewGuid()
        Type     = eventType // this is the 'semantic name', see TypeNaming.name.
        Data     = data
        Metadata = metaData
        IsJson   = true }

    /// The prepared EventData to write
    let preparedEvts = es |> List.map (prepareData commitHeaders)

    let chunk = List.chunk PageSize

    // depending on the length of the list to be written
    match es.Length with
    // few enough events to write in one go
    | len when uint32 len <= PageSize ->
      async {
        let! appended = preparedEvts |> Conn.append conn None streamId expected
        match appended with
        | Choice1Of2 _ -> return ()
        | Choice2Of2 ex -> return raise (StreamWrongExpectedVersion(ex)) }
    // chunk the event stream and write them in a transaction
    | _ ->
      async {
        let! started = conn |> Conn.startTx id expected None
        match started with
        | Choice1Of2 tx ->
          for ch in Seq.unfold chunk preparedEvts do
            do! ch |> Tx.write tx
          do! tx |> Tx.commit
        | Choice2Of2 ex ->
          return raise (StreamWrongExpectedVersion(ex)) }

  /// Creates event store based repository.
  /// The 'serialize' function returns the type name and the serialised byte array
  [<CompiledName("Make")>]
  let make (conn : Connection)
           (serialize   : obj -> string * byte array)
           (deserialize : Type * byte array -> obj)
           =

    load deserialize conn, commit serialize conn