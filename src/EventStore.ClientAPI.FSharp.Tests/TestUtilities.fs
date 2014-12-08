﻿module TestUtilities

open System.Reflection
open System.Net
open System.IO
open System.Threading

open EventStore.Core
open EventStore.ClientAPI
open EventStore.ClientAPI.Embedded

/// Execute f in a context where you can bind to the event store
let with_embedded_es f =
  let node = EmbeddedVNodeBuilder.AsSingleNode()
                                  .OnDefaultEndpoints()
                                  .RunInMemory()
                                  .RunProjections(ProjectionsMode.All)
                                  .WithWorkerThreads(16)
                                  .Build()
  use are = new AutoResetEvent(false)
  use sub =
    node.NodeStatusChanged.Subscribe(fun args ->
      args.NewVNodeState
      |> function
         | Data.VNodeState.Shutdown -> are.Set() |> ignore
         | _                        -> ())
  try
    printfn "starting embedded EventStore"
    node.Start()
    f()
  finally
    printfn "stopping embedded EventStore"
    node.Stop()
    are.WaitOne() |> ignore
    printfn "stopped embedded EventStore"

let with_real_es f = f ()
    
let with_connection factory f =
  factory <| fun _ ->
    let conn =
      Conn.configureStart()
      |> Conn.configureEnd (IPEndPoint(IPAddress.Loopback, 1113))
    conn |> Conn.connect |> Async.RunSynchronously
    try f conn
    finally conn |> Conn.close

let with_connection' f =
  with_connection with_embedded_es f

let resource name =
  let assembly = Assembly.GetExecutingAssembly ()
  let rname = sprintf "%s" name
  use stream = assembly.GetManifestResourceStream rname
  if stream = null then
    let list =
      assembly.GetManifestResourceNames()
      |> Array.fold (fun s t -> sprintf "%s\n - %s" s t) ""
    failwithf "couldn't find resource named '%s', from: %s" rname list
  else
    use reader = new StreamReader(stream)
    reader.ReadToEnd ()