module LifeOfAProgrammer

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
  | CodeLikeHell -> [ LostAnHourInVimVersusEmacsWar; MetAYakAndShavedIt ]
  | AvoidCodeSmell -> [ ReceivedUniversalAcclaim ]
  | PublishNuget -> [ GotBrokenPR; ReceivedHateOnMailingList ]

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