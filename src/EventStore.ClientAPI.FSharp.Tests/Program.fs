module Program

open EventStore.ClientAPI
open EventStore.ClientAPI.Common.Log

open Logary
open Logary.Configuration
open Logary.Targets

open Fuchu

[<EntryPoint>]
let main args =
  use logary =
    withLogary' "tests" (
      withTargets [
        Console.create Console.empty "console"
      ] >>
      withRules [
        Rule.createForTarget "console"
      ]
    )
  TestUtilities.lm := (Some logary)

  defaultMainThisAssembly args