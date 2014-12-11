module TestUtilities

open System.Reflection
open System.Net
open System.IO
open System.Threading

open Fuchu

open EventStore.Core
open EventStore.Core.Messages
open EventStore.Core.Bus
open EventStore.ClientAPI
open EventStore.ClientAPI.Embedded
open EventStore.ClientAPI.Common.Log

open NLog
open NLog.Config
open NLog.Targets

let lm : (Logary.LogManager option ref) = ref None

let conf_nlog () =
  let config = match LogManager.Configuration with | null -> new LoggingConfiguration () | cfg -> cfg
  use ct = new ConsoleTarget()
  config.AddTarget("console", ct)
  let rule = new LoggingRule("*", LogLevel.Trace, ct)
  config.LoggingRules.Add rule

  LogManager.Configuration <- config

/// Execute f in a context where you can bind to the event store
let with_embedded_es f =
  conf_nlog ()
  let node = EmbeddedVNodeBuilder.AsSingleNode()
                                  .OnDefaultEndpoints()
                                  .RunInMemory()
                                  .RunProjections(ProjectionsMode.All)
                                  .WithWorkerThreads(16)
                                  .Build()

  try
    printfn "starting embedded EventStore"
    node.Start()
    f()
  finally
    printfn "stopping embedded EventStore"
    use stopped = new AutoResetEvent(false)
    node.MainBus.Subscribe(
      new AdHocHandler<SystemMessage.BecomeShutdown>(
        fun m -> stopped.Set() |> ignore))
    node.Stop()
    if not (stopped.WaitOne(20000)) then
      Tests.failtest "couldn't stop ES within 20000 ms"
    else
      printfn "stopped embedded EventStore"

let with_real_es f = f ()
    
let with_connection factory f =
  factory <| fun _ ->
    let conn =
      ConnectionSettings.configureStart()
      |> ConnectionSettings.useCustomLogger (LogaryLogger((!lm).Value.GetLogger("EventStore")))
      |> ConnectionSettings.configureEnd (IPEndPoint(IPAddress.Loopback, 1113))
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
