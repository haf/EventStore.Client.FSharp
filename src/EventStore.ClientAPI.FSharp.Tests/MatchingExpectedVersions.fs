module MatchingExpectedVersions

open System.Text.RegularExpressions

open Fuchu

open EventStore.ClientAPI

let input = "Append failed due to WrongExpectedVersion. Stream: programmers-1, Expected version: -1"

let regex = Repo.VersionRegex

[<Tests>]
let tests =
  testList "matching from string" [
    testCase "can match stream" <| fun _ ->
      let ms = Regex.Match(input, regex)
      Assert.Equal("", true, ms.Success)
      Assert.Equal("correct stream name", "programmers-1", ms.Groups.[1].Value)
    testCase "can match version no" <| fun _ ->
      let ms = Regex.Match(input, regex)
      Assert.Equal("correct version no", "-1", ms.Groups.[2].Value)
  ]