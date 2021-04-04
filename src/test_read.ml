external pread_int : Unix.file_descr -> int -> bytes -> int -> int -> int
  = "caml_index_pread_int"

external pread_int_old : Unix.file_descr -> int64 -> bytes -> int -> int -> int
  = "caml_index_pread_old"

let pread ~fd ~fd_offset ~buffer ~buffer_offset ~length =
  pread_int fd fd_offset buffer buffer_offset length

let pread_64 ~fd ~fd_offset ~buffer ~buffer_offset ~length =
  pread_int_old fd fd_offset buffer buffer_offset length

let file = "/Users/icristes/Documents/index-tezos-bindings/index_BL/index/data"

let printable buffer =
  Bytes.unsafe_to_string buffer
  |> String.to_seq
  |> Seq.map Char.escaped
  |> List.of_seq
  |> String.concat ""

let () =
  let fd = Unix.openfile file Unix.[ O_EXCL; O_CLOEXEC; O_RDONLY ] 0o644 in
  let length = 3555 in
  let buffer = Bytes.create length in
  let _w = pread ~fd ~fd_offset:4477819804 ~buffer ~buffer_offset:0 ~length in
  Printf.printf "read %s\n" (printable buffer);
  let buffer = Bytes.create length in
  let _w =
    pread_64 ~fd ~fd_offset:4477819804L ~buffer ~buffer_offset:0 ~length
  in
  Printf.printf "read_64 %s\n" (printable buffer);
  Unix.close fd
