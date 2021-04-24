module Seq = struct
  include Seq

  (* Backported from ocaml 4.11 *)
  let rec unfold f u () =
    match f u with None -> Nil | Some (x, u') -> Cons (x, unfold f u')

  let mapi f s =
    let i = ref (-1) in
    Seq.map
      (fun v ->
        incr i;
        f !i v)
      s

  let mapi64 f s =
    let i = ref (-1L) in
    Seq.map
      (fun v ->
        i := Int64.succ !i;
        f !i v)
      s

  let with_lookahead : int -> la:('a -> unit) -> 'a Seq.t -> 'a Seq.t =
   fun lookahead_distance ~la seq ->
    let rec aux (seq_opt, q) =
      match (seq_opt, Queue.length q) with
      | None, 0 -> None
      | None, _ -> Some (Queue.pop q, (None, q))
      | Some _, len when len >= lookahead_distance ->
          Some (Queue.pop q, (seq_opt, q))
      | Some seq, _ -> (
          Seq.(
            match seq () with
            | Nil -> aux (None, q)
            | Cons (v, seq) ->
                la v;
                Queue.push v q;
                aux (Some seq, q)))
    in
    unfold aux (Some seq, Queue.create ())
end

module J = struct
  type key = string [@@deriving yojson]

  type op =
    | Clear
    | Flush
    | Mem of key * bool
    | Find of key * bool
    | Ro_mem of key * bool
    | Ro_find of key * bool
    | Add of key * (int64 * int * char)
  [@@deriving yojson]

  let read_ops path =
    let parse_op :
        Yojson.Safe.t -> (op, string) Ppx_deriving_yojson_runtime.Result.result
        =
     fun op -> op_of_yojson op
    in
    let json = Yojson.Safe.stream_from_file path in
    let aux : unit -> (op * unit) option =
     fun () ->
      match Stream.next json with
      | exception Stream.Failure ->
          Printf.printf "Done reading json\n%!";
          None
      | op -> (
          match parse_op op with
          | Ok x -> Some (x, ())
          | Error s -> Fmt.failwith "error op_of_yojson %s\n%!" s)
    in
    Seq.unfold aux ()
end

module R = struct
  type key = string [@@deriving repr]

  type op =
    | Clear
    | Flush
    | Mem of key * bool
    | Find of key * bool
    | Ro_mem of key * bool
    | Ro_find of key * bool
    | Add of key * (int64 * int * char)
  [@@deriving repr]
end

let r_of_j = function
  | J.Clear -> R.Clear
  | J.Flush -> R.Flush
  | J.Mem (k, b) -> R.Mem (k, b)
  | J.Find (k, b) -> R.Find (k, b)
  | J.Ro_mem (k, b) -> R.Ro_mem (k, b)
  | J.Ro_find (k, b) -> R.Ro_find (k, b)
  | J.Add (k, v) -> R.Add (k, v)

let _prefix = "/Users/icristes/Documents/index-tezos-bindings"

let radix = "trace_index"

let p0 = radix ^ ".json"

let p1 = radix ^ ".repr"

let main_convert () =
  let encode_bin = Repr.encode_bin R.op_t |> Repr.unstage in
  let encode_prefix = Repr.(encode_bin int32) |> Repr.unstage in
  if Sys.file_exists p1 then Fmt.failwith "Destination exists, remove it. %s" p1;
  let channel = open_out p1 in
  let buffer = Buffer.create (1024 * 1024 * 10) in
  let ppf = Format.formatter_of_out_channel channel in
  let print s = Format.fprintf ppf "%s" s in
  J.read_ops p0
  |> Seq.map r_of_j
  |> Seq.iter (fun op ->
         Buffer.clear buffer;
         encode_bin op (Buffer.add_string buffer);
         let s = Buffer.contents buffer in
         let len = String.length s |> Int32.of_int in
         encode_prefix len print;
         print s);
  Format.fprintf ppf "%!";
  close_out channel

let () =
  Printexc.record_backtrace true;
  main_convert ()
