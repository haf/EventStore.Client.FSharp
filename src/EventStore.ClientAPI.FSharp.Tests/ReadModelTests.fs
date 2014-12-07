module Tests.ReadModelTests

open System
open System.Net
open System.Reflection
open System.Threading
open System.Threading.Tasks
open System.IO

open Fuchu

open TestUtilities

open LifeOfAProgrammer

open EventStore.ClientAPI
open EventStore.ClientAPI.Exceptions
open EventStore.ClientAPI.AsyncHelpers
open EventStore.ClientAPI.SystemData
open EventStore.ClientAPI.Common.Log

open Intelliplan.JsonNet

let run_t (t : Task) = t |> Async.AwaitTask |> Async.RunSynchronously

let rec wait_until_no_exn (f : unit -> Task) =
  try f () |> run_t
  with
  | :? ProjectionCommandFailedException as e
    when e.Message.Contains("Not yet ready.") ->
    printfn "... waiting for server to get ready ..."
    async { do! Async.Sleep 1000 } |> Async.RunSynchronously
    wait_until_no_exn f

[<Tests>]
let roundtrip_tests =
  let ep = new IPEndPoint(IPAddress.Loopback, 2113)
  let logger = ConsoleLogger()
  let timeout = TimeSpan.FromSeconds 8.
  let default_creds = UserCredentials ("admin", "changeit")

  let eventstore_impl = with_real_es

  testList "read your writes" [
    testCase "can add read model" <| fun _ ->
      eventstore_impl <| fun _ ->
        let pm = ProjectionsManager(logger, ep, timeout)

        wait_until_no_exn <| fun _ ->
          pm.CreateContinuousAsync("EventCounting", resource "EventCounting.js", default_creds)
        
        pm.CreateContinuousAsync("IsHeAManagerYet", resource "IsHeAManagerYet.js", default_creds)
        |> run_t

    testCase "can execute commands against AR" <| fun _ ->
      with_connection eventstore_impl <| fun conn ->
        let evts =
          CodeLikeHell
          |> LifeOfAProgrammer.write conn "programmer-1" NoStream
          |> Async.RunSynchronously

        match evts with
        | [_; MetAYakAndShavedIt] -> ()
        | es -> Tests.failtest "got %A, expected something else" es

    testCase "smoke: expected URN" <| fun _ ->
      let n = TypeNaming.nameObj MetAYakAndShavedIt
      Assert.Equal("expected name", "urn:Tests:LifeOfAProgrammer_Evt|MetAYakAndShavedIt", n)

    testCase "read projection" <| fun _ ->
      let ensure_projections () =
        let pm = ProjectionsManager(logger, ep, timeout)
        wait_until_no_exn <| fun _ ->
          pm.CreateContinuousAsync("EventCounting", resource "EventCounting.js", default_creds)
        pm.CreateContinuousAsync("IsHeAManagerYet", resource "IsHeAManagerYet.js", default_creds) |> run_t
        pm

      let write_code_command conn =
        CodeLikeHell
        |> LifeOfAProgrammer.write conn "programmers-1" NoStream
        |> Async.RunSynchronously
        |> ignore

      with_connection eventstore_impl <| fun conn ->
        let pm = ensure_projections ()
        write_code_command conn
        let state = pm.GetStateAsync("EventCounting", default_creds) |> Async.AwaitTask |> Async.RunSynchronously
        printfn "state: %s" state
    ]