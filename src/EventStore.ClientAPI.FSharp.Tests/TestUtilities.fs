module TestUtilities

open EventStore.Core
open EventStore.ClientAPI.Embedded

/// Execute f in a context where you can bind to the event store
let with_eventstore f =
  let node = EmbeddedVNodeBuilder.AsSingleNode()
                                  .OnDefaultEndpoints()
                                  .RunInMemory()
                                  .RunProjections(ProjectionsMode.All)
                                  .WithWorkerThreads(16)
                                  .Build()
  node.Start()
  try f()
  finally node.Stop()