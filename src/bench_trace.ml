let reporter ?(prefix = "") () =
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let ppf = match level with Logs.App -> Fmt.stdout | _ -> Fmt.stderr in
    let with_stamp h _tags k fmt =
      let dt = Unix.gettimeofday () in
      Fmt.kpf k ppf
        ("%s%+04.0fus %a %a @[" ^^ fmt ^^ "@]@.")
        prefix dt Logs_fmt.pp_header (level, h)
        Fmt.(styled `Magenta string)
        (Logs.Src.name src)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k fmt
  in
  { Logs.report }

let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (reporter ());
  ()

let with_timer f =
  let t0 = Sys.time () in
  let a = f () in
  let t1 = Sys.time () -. t0 in
  (t1, a)

let with_progress_bar ~message ~n ~unit =
  let bar =
    let w =
      if n = 0 then 1
      else float_of_int n |> log10 |> floor |> int_of_float |> succ
    in
    let pp fmt i = Format.fprintf fmt "%*Ld/%*d %s" w i w n unit in
    let pp f = f ~width:(w + 1 + w + 1 + String.length unit) pp in
    Progress_unix.counter ~mode:`ASCII ~width:79 ~total:(Int64.of_int n)
      ~message ~pp ()
  in
  Progress_unix.with_reporters bar

module FSHelper = struct
  let file f =
    try (Unix.stat f).st_size with Unix.Unix_error (Unix.ENOENT, _, _) -> 0

  let index root =
    let index_dir = Filename.concat root "index" in
    let a = file (Filename.concat index_dir "data") in
    let b = file (Filename.concat index_dir "log") in
    let c = file (Filename.concat index_dir "log_async") in
    (a + b + c) / 1024 / 1024

  let size root = index root

  let get_size root = size root

  let rm_dir root =
    if Sys.file_exists root then (
      let cmd = Printf.sprintf "rm -rf %s" root in
      Logs.info (fun l -> l "exec: %s" cmd);
      let _ = Sys.command cmd in
      ())
end

let decoded_seq_of_encoded_chan_with_prefixes :
    'a Repr.ty -> in_channel -> 'a Seq.t =
 fun repr channel ->
  let decode_bin = Repr.decode_bin repr |> Repr.unstage in
  let decode_prefix = Repr.(decode_bin int32 |> unstage) in
  let produce_op () =
    try
      (* First read the prefix *)
      let prefix = really_input_string channel 4 in
      let len', len = decode_prefix prefix 0 in
      assert (len' = 4);
      let len = Int32.to_int len in
      (* Then read the repr *)
      let content = really_input_string channel len in
      let len', op = decode_bin content 0 in
      assert (len' = len);
      Some (op, ())
    with End_of_file -> None
  in
  Seq.unfold produce_op ()

type int63 = Encoding.int63 [@@deriving repr]

type config = { commit_data_file : string; root : string }

module Trace = struct
  type key = string [@@deriving repr]

  type op =
    | Flush
    | Mem of key * bool
    | Find of key * bool
    | Add of key * (int63 * int * char)
  [@@deriving repr]

  let open_ops_sequence path : op Seq.t =
    let chan = open_in_bin path in
    decoded_seq_of_encoded_chan_with_prefixes op_t chan
end

module Benchmark = struct
  type result = { time : float; size : int }

  let run config f =
    let time, res = with_timer f in
    let size = FSHelper.get_size config.root in
    ({ time; size }, res)

  let pp_results ppf result =
    Format.fprintf ppf "Total time: %f@\nSize on disk: %d M" result.time
      result.size
end

module type S = sig
  include Index.S

  val v : string -> t

  val close : t -> unit
end

module Index = struct
  open Encoding
  module Index = Index_unix.Make (Key) (Val) (Index.Cache.Unbounded)
  include Index

  let cache = Index.empty_cache ()

  let v root =
    Index.v ~cache ~readonly:false ~fresh:false ~log_size:500_000 root

  let close t = Index.close t
end

module Bench_suite
    (Store : S
               with type key = Encoding.Hash.t
                and type value = int63 * int * char) =
struct
  let key_to_hash k =
    match Encoding.Hash.of_string k with
    | Ok k -> k
    | Error (`Msg m) -> Fmt.failwith "error decoding hash %s" m

  let add_operation store op_seq () =
    with_progress_bar ~message:"Replaying trace" ~n:65249800 ~unit:"operations"
    @@ fun progress ->
    let rec aux op_seq i =
      match op_seq () with
      | Seq.Nil -> i
      | Cons (op, op_seq) ->
          let () =
            match op with
            | Trace.Flush ->
                Logs.debug (fun l -> l "flush");
                Store.flush store
            | Mem (k, b) ->
                Logs.debug (fun l -> l "mem %s %b" k b);
                let k = key_to_hash k in
                let b' = Store.mem store k in
                if b <> b' then
                  Fmt.failwith "Operation mem %a expected %b got %b"
                    (Repr.pp Encoding.Key.t) k b b'
            | Find (k, b) ->
                Logs.debug (fun l -> l "find %s %b" k b);
                let k = key_to_hash k in
                let b' =
                  try
                    let _ = Store.find store k in
                    true
                  with Not_found -> false
                in
                if b <> b' then
                  Fmt.failwith "Operation find %a expected %b got %b"
                    (Repr.pp Encoding.Key.t) k b b'
            | Add (k, v) ->
                Logs.debug (fun l -> l "add %s" k);
                let k = key_to_hash k in
                Store.replace store k v
          in
          progress Int64.one;
          aux op_seq (i + 1)
    in
    aux op_seq 0

  let run_read_trace config =
    let op_seq = Trace.open_ops_sequence config.commit_data_file in
    let store = Store.v config.root in

    let result, nb_ops = add_operation store op_seq |> Benchmark.run config in

    let () = Store.close store in

    fun ppf ->
      Format.fprintf ppf "Tezos trace for %d nb_ops @\nResults: @\n%a@\n" nb_ops
        Benchmark.pp_results result
end

module Bench = Bench_suite (Index)

let main () commit_data_file =
  Printexc.record_backtrace true;
  Random.self_init ();
  let config = { commit_data_file; root = "index_BL" } in
  let results = Bench.run_read_trace config in
  Logs.app (fun l -> l "%a@." (fun ppf f -> f ppf) results)

open Cmdliner

let commit_data_file =
  let doc =
    Arg.info ~docv:"PATH" ~doc:"Trace of Tezos operations to be replayed." []
  in
  Arg.(required @@ pos 0 (some string) None doc)

let setup_log =
  Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let main_term = Term.(const main $ setup_log $ commit_data_file)

let () =
  let man =
    [
      `S "DESCRIPTION";
      `P
        "Benchmarks for index operations. Requires traces of operations \
         (/data/ioana/trace_index.repr) and initial index store \
         (/data/ioana/index_BL.tar.gz)";
    ]
  in
  let info =
    Term.info ~man ~doc:"Benchmarks for index operations" "bench-index"
  in
  Term.exit @@ Term.eval (main_term, info)
