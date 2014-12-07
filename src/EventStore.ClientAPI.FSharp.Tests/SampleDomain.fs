module Tests.LifeOfAProgrammer

open EventStore.ClientAPI
open Intelliplan.JsonNet

type Cmd =
  | CodeLikeHell
  | AvoidCodeSmell
  | PublishNuget

type Evt =
  | ReceivedUniversalAcclaim
  | GotBrokenPR
  | ReceivedHateOnMailingList
  | FoughtWindmill
  | MetAYakAndShavedIt
  | MetAHerdOfYaksAndShavedThemALL
  | LostAnHourInVimVersusEmacsWar
  | LostAWeekFromSegfault

let exec state = function
  | CodeLikeHell   -> [ LostAnHourInVimVersusEmacsWar; MetAYakAndShavedIt ]
  | AvoidCodeSmell -> [ ReceivedUniversalAcclaim ]
  | PublishNuget   -> [ GotBrokenPR; ReceivedHateOnMailingList ]

type CoderState =
  | HappyAndFulfilled
  | NormalState
  | Schizophrenic
  | Bipolar
  | Depressed
  | OnSpeed
  | MicroManaged

let apply state = function
  | ReceivedUniversalAcclaim       -> HappyAndFulfilled
  | GotBrokenPR                    -> Depressed
  | ReceivedHateOnMailingList      -> Schizophrenic
  | FoughtWindmill                 -> NormalState
  | MetAYakAndShavedIt             -> NormalState
  | MetAHerdOfYaksAndShavedThemALL -> NormalState
  | LostAnHourInVimVersusEmacsWar  -> Bipolar
  | LostAWeekFromSegfault          -> MicroManaged

let aggregate =
  { Aggregate.zero  = NormalState
    Aggregate.apply = apply
    Aggregate.exec  = exec }

let write conn id version cmd =
  let load, commit = Serialisation.serialiser ||> Repo.make conn
  let handler = Aggregate.makeHandler aggregate load commit
  async {
    let! events, write = handler (id, version) cmd
    do! write
    return events }
