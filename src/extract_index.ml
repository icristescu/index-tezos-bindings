open Cmdliner
open Re.Str

let file =
  let doc = Arg.info ~doc:"File." [ "f"; "file" ] in
  Arg.(value @@ opt string "logs" doc)

let run_id =
  let doc = Arg.info ~doc:"Run id." [ "i"; "id" ] in
  Arg.(value @@ opt int 1 doc)

let remove_ro =
  let doc = Arg.info ~doc:"Remove RO entries" [ "ro" ] in
  Arg.(value & flag @@ doc)

let read_file infile outfile extract =
  let chan = open_in infile in
  let oc = open_out outfile in
  try
    while true do
      let line = input_line chan in
      match extract line with None -> () | Some l -> Printf.fprintf oc "%s\n" l
    done
  with End_of_file ->
    close_in chan;
    close_out oc

let context_lines s =
  let info = "\\[INFO\\]" in
  let index = "\\[index\\]" in
  try
    let _ = search_forward (regexp info) s 0 in
    let pos = search_forward (regexp index) s 0 in
    if not (string_match (regexp "\\[") s (pos + 8)) then raise Not_found;
    let ls = string_after s pos |> split (regexp "[ ]+") in
    (* Fmt.epr "found at pos %d in line %s ls = %a\n" pos s Fmt.(list string) ls; *)
    Some (try List.nth ls 1 with e -> Fmt.epr "Failure in %s" s; raise e)
  with Not_found -> None

let remove_lines s =
  let ro = "Ro_" in
  try
    let _ = search_forward (regexp ro) s 0 in
    None
  with Not_found -> Some s

let main file run_id remove_ro =
  let run_id = Printf.sprintf "%d" run_id in
  let out_file = "rw_index.json" in
  if remove_ro then read_file file out_file remove_lines
  else
  (let out_file = run_id ^ "-index.json" in
  read_file file out_file context_lines)

let main_term = Term.(const main $ file $ run_id $ remove_ro)

let () =
  let info = Term.info "Extract from the logs of a tezos node" in
  Term.exit @@ Term.eval (main_term, info)
