let reporter () =
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let with_stamp h _tags k fmt =
      let dt = Sys.time () in
      Fmt.kpf k Fmt.stderr
        ("%+04.0fus %a %a @[" ^^ fmt ^^ "@]@.")
        dt
        Fmt.(styled `Magenta string)
        (Logs.Src.name src) Logs_fmt.pp_header (level, h)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k fmt
  in
  { Logs.report }

let root = "/Users/icristes/Documents/index-tezos-bindings/index_BL/"

let target = "CoUybPX6HTPcaeqFo2XJM5ubpZc1PE9nVqDhEw8tMRmAyBjU7jmo"

module Stats = Index.Stats
open Encoding
module Index = Index_unix.Make (Key) (Val) (Index.Cache.Unbounded)

let cache = Index.empty_cache ()

let v = Index.v ~cache

let close t = Index.close t

let run_index () =
  Logs.debug (fun l -> l "open store");
  let index = Index.v ~fresh:false ~readonly:true ~log_size:500_000 root in
  match Encoding.Hash.of_string target with
  | Error (`Msg m) -> failwith m
  | Ok key ->
      let v = Index.find index key in
      Logs.app (fun l -> l "key found, value = %a" (Repr.pp Val.t) v);
      close index

let () =
  Logs.set_level (Some Logs.App);
  Logs.set_reporter (reporter ());
  Printexc.record_backtrace true;
  run_index ()
