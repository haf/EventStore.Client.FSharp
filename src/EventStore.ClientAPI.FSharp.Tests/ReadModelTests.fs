module ReadModelTests

open System
open System.Net
open System.Reflection
open System.IO

open Fuchu

open TestUtilities

open EventStore.ClientAPI
open EventStore.ClientAPI.AsyncHelpers
open EventStore.ClientAPI.SystemData
open EventStore.ClientAPI.Common.Log

let file name =
  let assembly = Assembly.GetExecutingAssembly ()
  let rname = sprintf "EventStore.ClientAPI.FSharp.Tests.%s" name
  use stream = assembly.GetManifestResourceStream rname
  if stream = null then
    failwithf "couldn't find resource named '%s'" rname
  else
    use reader = new StreamReader(stream)
    reader.ReadToEnd ()

[<Tests>]
let tests =
  let ep = new IPEndPoint(IPAddress.Loopback, 2113)
  let logger = ConsoleLogger()
  let timeout = TimeSpan.FromSeconds 8.
  let default_creds = UserCredentials ("admin", "changeit")

  testList "read your writes" [
    testCase "can add read model" <| fun _ ->
      with_eventstore <| fun _ ->
        let pm = ProjectionsManager(logger, ep, timeout)
        pm.CreateContinuousAsync("CurrentFeelings", file "CurrentFeelings.js", default_creds)
        |> Async.AwaitTask |> Async.RunSynchronously
        pm.CreateContinuousAsync("IsHeAManagerYet", file "IsHeAManagerYet.js", default_creds)
        |> Async.AwaitTask |> Async.RunSynchronously
    ]