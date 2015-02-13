module ApiTests

open System
open System.Security.Cryptography

open Fuchu

open EventStore.ClientAPI

[<Tests>]
let utilities =
  testList "utilities unit tests" [

    testCase "Option.fromNullable null" <| fun _ ->
      let n : Nullable<int> = Nullable()
      Assert.Equal("should be None", None, Helpers.Option.fromNullable n)
    testCase "Option.fromNullable not null" <| fun _ ->
      let n : Nullable<int> = Nullable 4
      Assert.Equal("should be Some 4", Some 4, Helpers.Option.fromNullable n)

    testCase "Helpers.memoize" <| fun _ ->
      let value = ref 0
      let f : unit -> int =
        fun () ->
          value := !value + 1
          !value
      let f' = Helpers.memoize f
      Assert.Equal("should be one after first call",
                   1, f' ())
      Assert.Equal("should be one after second call, too",
                   1, f' ())
      Assert.Equal("but calling the original function increments",
                   2, f ())
    ]


open Events

type A(y) =
  member x.X = y

[<Tests>]
let recorded_event =
  testList "handling RecordedEvent" [

    testCase "empty RecordedEvent" <| fun _ ->
      RecordedEvent.Empty |> ignore

    testCase "nonempty RecordedEvent" <| fun _ ->
      let epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)
      let value = Convert.ToInt64((DateTime(2015, 02, 27) - epoch).TotalSeconds)
      { RecordedEvent.Empty with
         Created = value * 1000L }
      |> unwrapRecordedEvent
      |> fun re ->
        Assert.Equal("after unwrap", value * 1000L, re.CreatedEpoch)
    ]

[<Tests>]
let units =
  testList "eventstore client api unit test" [
    testCase "empty EventData" <| fun _ ->
      EventData.Empty |> ignore
    testCase "empty RecordedEvent" <| fun _ ->
      RecordedEvent.Empty |> ignore
    testCase "EventData from RecordedEvent" <| fun _ ->
      EventData.FromRecordedEvent true RecordedEvent.Empty |> ignore
    testCase "RecordedEvent from EventData" <| fun _ ->
      RecordedEvent.FromEventData "stream id" 1234u 0L EventData.Empty
      |> ignore
    testCase "empty ResolvedEvent" <| fun _ ->
      ResolvedEvent.Empty |> ignore
    testCase "empty ResolvedEvent" <| fun _ ->
      ResolvedEvent.Empty.OriginalEventNumber |> ignore

    testCase "empty EventReadResult" <| fun _ ->
      EventReadResult.Empty |> ignore
    testCase "empty StreamEventsSlice" <| fun _ ->
      StreamEventsSlice.Empty |> ignore
    testCase "empty AllEventsSlice" <| fun _ ->
      AllEventsSlice.Empty |> ignore
    testCase "clr type name from type" <| fun _ ->
      let t = typeof<A>
      Assert.Equal("should eq module plus type name",
                   "ApiTests+A, EventStore.ClientAPI.FSharp.Tests",
                   t.ToPartiallyQualifiedName ())
    testCase "compute sha1 on empty array" <| fun _ ->
      use sha = SHA1.Create()
      Assert.Equal("160 bits size", 160, sha.HashSize)
      let subject = sha.ComputeHash [||]
      Assert.Equal("should be 160 bits", 160, subject.Length * 8)
      Assert.NotEqual("hash of empty array is not zero(oes)",
                      [||], subject)
    testCase "hash a string into a guid" <| fun _ ->
      let subject = "Hello World, of course!"
      let guid    = toGuid subject
      Assert.NotEqual("not empty", Guid.Empty, guid)
    testCase "can create EventData from some object" <| fun _ ->
      let data = A 42
      let subject = EventData.From data "EventA"
      Assert.Equal("should have id from serialized object",
                   data |> toJson |> toGuid,
                   subject.Id)
      Assert.Equal("should have type that was passed",
                   "EventA", subject.Type)
      Assert.Equal("should have json serialized data",
                   data |> toJsonB,
                   subject.Data)
      Assert.Equal("should say it's JSON",
                   true, subject.IsJson)

  ]