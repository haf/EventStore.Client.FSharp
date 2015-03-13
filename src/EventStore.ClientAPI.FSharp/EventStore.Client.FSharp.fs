namespace EventStore.ClientAPI

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
    //   : (('a -> 'b) -> ('c -> Async<'a>) -> 'c -> Async<'b>)
    let MapCompose fMap fOrig c =
      async {
        let! res = fOrig c
        return fMap res }

    let (<<!) = MapCompose

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
                                            * StreamCheckpoint
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
            let count = validUint count
            let startEvent =
                match start with
                | Specific i -> validUint i
                | LastInStream -> -1
            y.ReadStreamEventsBackwardAsync(streamId, startEvent, count,
                                            (match resolveLinks with ResolveLinks r -> r),
                                            Option.noneIsNull userCredentials)

          member x.ReadStreamEventsForwardAsync (streamId, start, count, resolveLinks, userCredentials) =
            let count = validUint count
            let startEvent =
                match start with
                | Specific i -> validUint i
                | LastInStream -> -1
            y.ReadStreamEventsForwardAsync(streamId, startEvent, count,
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
      FromPosition  : Position
      NextPosition  : Position
      ReadDirection : ReadDirection }

  type AllEventsSlice with
    static member Empty = { Events = []; FromPosition = Position(); NextPosition = Position(); ReadDirection = Forward }

  let unwrapAllEventsSlice (e : AllEventsSlice) =
    newAES (unwrapReadDirection e.ReadDirection)
      e.FromPosition
      e.NextPosition
      (e.Events |> List.map unwrapResolvedEvent |> List.toArray)

  let wrapAllEventsSlice (e : EventStore.ClientAPI.AllEventsSlice) =
    { Events        = e.Events |> List.ofArray |> List.map wrapResolvedEvent
      FromPosition  = e.FromPosition
      NextPosition  = e.FromPosition
      ReadDirection = e.ReadDirection |> wrapReadDirection }

/// Thin F# wrapper on top of EventStore.Client's API. Doesn't
/// translate any structures, but enables currying and F# calling conventions.
/// The EventStoreConnection class is responsible for maintaining a full-duplex connection between the client and the event store server. EventStoreConnection is thread-safe, and it is recommended that only one instance per application is created.
/// This module has its methods sorted by the 'Connection' interface.
module Conn =

  open Helpers
  open AsyncHelpers

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
  let subscribe (c : Connection)
                streamId
                resolveLinks
                eventAppeared
                subscriptionDropped
                userCredentials =
    c.SubscribeToStreamAsync(streamId, resolveLinks,
                             eventAppeared |> functor2 Types.wrapResolvedEvent |> action2,
                             subscriptionDropped |> Option.map action3,
                             userCredentials)
    |> Async.AwaitTask

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
  let subscribeFrom (c : Connection)
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

  /// SubscribeToAll
  let subscribeAll (c : Connection)
                   resolveLinks
                   eventAppeared
                   subscriptionDropped
                   userCredentials =
    c.SubscribeToAllAsync(resolveLinks,
                          eventAppeared |> functor2 Types.wrapResolvedEvent |> action2,
                          subscriptionDropped |> Option.map action3,
                          userCredentials)
    |> Async.AwaitTask

  /// SubscribeToAllFrom
  let subscribeAllFrom (c : Connection) fromPos resolveLinks eventAppeared 
    liveProcessingStarted subscriptionDropped userCredentials =
    c.SubscribeToAllFrom(fromPos, resolveLinks,
                         eventAppeared |> functor2 Types.wrapResolvedEvent |> action2,
                         liveProcessingStarted |> Option.map action,
                         subscriptionDropped |> Option.map action3,
                         userCredentials)

module ConnectionSettings =
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

  open Helpers
  open AsyncHelpers

  open EventStore.ClientAPI
  open EventStore.ClientAPI.Exceptions

  open FSharp.Data

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

  // reflection error with following line:
  type Status = JsonProvider<"get_status_sample.json">

  let getStatus ctx name =
    let pm = mkManager ctx
    async {
      let! res =
        pm.GetStatusAsync(name, ctx.creds)
        |> Async.AwaitTask
        |> handleHttpCodes ctx.logger
      return res |> Option.map Status.Parse
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
        | Some status when status.Status <> "Running" ->
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
      raise (Exception("exception in wait_for_init", origErr))
    }
 
  let ensureContinuous ctx name query =
    let pm = mkManager ctx
    async {
      ctx.logger.Debug "calling get_status"
      let! status = getStatus ctx name
      match status with
      | None -> 
        ctx.logger.Debug "calling create_continuous from 404 case"
        do! create_continuous ctx name query
      | Some status when status.Status <> "Running" ->
        ctx.logger.Debug "calling create_continuous from not Running case"
        do! create_continuous ctx name query
      | other ->
        ctx.logger.Debug (sprintf "got %O back" other)
        return ()
    }
