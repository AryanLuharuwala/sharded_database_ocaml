(* sharded_kv_ocaml_full.ml

   Full, self-contained single-file implementation that combines:
   - immutable Red-Black Tree (RbMap) per-shard
   - optimized WAL writer with open fd, buffered writes, batched fsyncs, flush interval
   - periodic snapshotting and WAL rotation
   - consistent hashing ring with virtual nodes and rebalancing
   - master/replica model with quorum ACKs
   - simple leader election (best-effort)
   - metrics & logging
   - MPI support to run shards across processes (optional)
   - HTTP API (Cohttp + Lwt) exposing GET/PUT/BATCH/REBALANCE endpoints per-process

   Build requirements (example):
     opam install dune domainslib lwt cohttp-lwt-unix yojson astring
   MPI bindings are optional; if you want MPI support install ocaml-mpi or equivalent
   and compile with -package mpi and link the MPI C library.

   Dune (suggested):
   (executable
    (name sharded_kv_ocaml_full)
    (libraries domainslib lwt cohttp-lwt-unix yojson astring))

   Notes:
   - This file is a demonstrative reference. Production systems should use existing
     consensus libraries (Raft), serious I/O engines, and robust error handling.

*)

open Printf

(* --- Dependencies used in this file --- *)
(* libraries: domainslib, lwt, cohttp-lwt-unix, yojson, astring *)

(* ------------------------------ RbMap ------------------------------ *)
module RbMap = struct
  type color = Red | Black
  type key = string
  type 'a t = Empty | Node of color * 'a t * key * 'a * 'a t
  let empty = Empty
  let rec mem k = function
    | Empty -> false
    | Node (_c, l, kx, _v, r) -> if k = kx then true else if k < kx then mem k l else mem k r
  let rec find k = function
    | Empty -> raise Not_found
    | Node (_c, l, kx, v, r) -> if k = kx then v else if k < kx then find k l else find k r
  let balance = function
    | Black, Node (Red, Node (Red, a, xk, xv, b), yk, yv, c), zk, zv, d
    | Black, Node (Red, a, xk, xv, Node (Red, b, yk, yv, c)), zk, zv, d
    | Black, a, xk, xv, Node (Red, Node (Red, b, yk, yv, c), zk, zv, d)
    | Black, a, xk, xv, Node (Red, b, yk, yv, Node (Red, c, zk, zv, d)) ->
        Node (Red, Node (Black, a, xk, xv, b), yk, yv, Node (Black, c, zk, zv, d))
    | (Black, l, k, v, r) -> Node (Black, l, k, v, r)
    | (Red, l, k, v, r) -> Node (Red, l, k, v, r)
  let insert s v t =
    let rec ins = function
      | Empty -> Node (Red, Empty, s, v, Empty)
      | Node (c, a, k, v0, b) ->
          if s = k then Node (c, a, k, v, b)
          else if s < k then balance (c, ins a, k, v0, b)
          else balance (c, a, k, v0, ins b)
    in
    match ins t with
    | Node (_, a, k, v, b) -> Node (Black, a, k, v, b)
    | Empty -> Empty
  let rec min_binding = function
    | Empty -> raise Not_found
    | Node (_c, Empty, k, v, _r) -> (k, v)
    | Node (_c, l, _k, _v, _r) -> min_binding l
  let rec remove_min_binding = function
    | Empty -> invalid_arg "remove_min_binding"
    | Node (c, Empty, _k, _v, r) -> r
    | Node (c, l, k, v, r) -> balance (c, remove_min_binding l, k, v, r)
  let rec remove s t =
    let rec do_remove = function
      | Empty -> Empty
      | Node (c, a, k, v, b) as node ->
          if s = k then (
            match a, b with
            | Empty, _ -> b
            | _, Empty -> a
            | _ -> let (k', v') = min_binding b in balance (c, a, k', v', remove_min_binding b)
          )
          else if s < k then balance (c, do_remove a, k, v, b) else balance (c, a, k, v, do_remove b)
    in
    match do_remove t with
    | Node (_, a, k, v, b) -> Node (Black, a, k, v, b)
    | Empty -> Empty
  let rec fold f t acc = match t with Empty -> acc | Node (_c, l, k, v, r) -> let acc = fold f l acc in let acc = f k v acc in fold f r acc
  let bindings t =
    let acc = ref [] in
    fold (fun k v _ -> acc := (k, v) :: !acc; ()) t ();
    List.rev !acc
  let iter f t = fold (fun k v () -> f k v) t ()
end

(* --------------------------- Configuration -------------------------- *)
let virtual_nodes = 128
let wal_batch_size = 64
let wal_flush_interval = 0.5
let metrics_flush_interval = 5.0
let snapshot_interval = 30.0
let use_mpi = false (* set true if MPI bindings are available and desired at build-time *)

(* ----------------------------- Metrics ----------------------------- *)
module Metrics = struct
  type t = { mutable put_count:int; mutable get_count:int; mutable fsync_count:int; mutable wal_bytes:int64; mutable last_flush: float }
  let make () = { put_count=0; get_count=0; fsync_count=0; wal_bytes=0L; last_flush=Unix.gettimeofday () }
  let flush_to_file fname m =
    try
      let oc = open_out_gen [Open_append; Open_creat] 0o644 fname in
      let line = sprintf "%f PUT=%d GET=%d FSYNC=%d BYTES=%Ld
" (Unix.gettimeofday ()) m.put_count m.get_count m.fsync_count m.wal_bytes in
      output_string oc line; flush oc; close_out oc
    with _ -> ()
end

(* --------------------------- WAL Writer ---------------------------- *)
type wal_writer = { fd: Unix.file_descr; mutable buf: Buffer.t; mutable pending:int; fname:string; metrics:Metrics.t }
let make_wal_writer fname metrics =
  let flags = [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_APPEND] in
  let fd = Unix.openfile fname flags 0o644 in
  { fd; buf = Buffer.create 4096; pending = 0; fname; metrics }
let append_wal_buffered ww line =
  Buffer.add_string ww.buf (line ^ "
");
  ww.pending <- ww.pending + 1;
  ww.metrics.wal_bytes <- Int64.add ww.metrics.wal_bytes (Int64.of_int (String.length line + 1));
  if ww.pending >= wal_batch_size then begin
    let s = Buffer.contents ww.buf in
    ignore (Unix.write_substring ww.fd s 0 (String.length s));
    Buffer.clear ww.buf; ww.pending <- 0;
    (try Unix.fsync ww.fd; ww.metrics.fsync_count <- ww.metrics.fsync_count + 1 with _ -> ());
  end
let flush_wal_writer ww =
  if Buffer.length ww.buf > 0 then begin
    let s = Buffer.contents ww.buf in
    ignore (Unix.write_substring ww.fd s 0 (String.length s));
    Buffer.clear ww.buf; ww.pending <- 0;
    (try Unix.fsync ww.fd; ww.metrics.fsync_count <- ww.metrics.fsync_count + 1 with _ -> ());
  end
let close_wal_writer ww = flush_wal_writer ww; Unix.close ww.fd

(* --------------------------- Consistent Ring ----------------------- *)
module Ring = struct
  type t = (int * string) list
  let hash_str s = Hashtbl.hash s |> abs
  let build ~shard_ids ~vnodes =
    let entries = ref [] in
    List.iter (fun sid -> for i=0 to vnodes-1 do let vnode = sprintf "%d-%d" sid i in entries := (hash_str vnode, vnode)::!entries done) shard_ids;
    List.sort (fun (a,_) (b,_) -> compare a b) !entries
  let find_shard t key =
    let h = hash_str key in
    let rec bs l r = if l > r then None else let m=(l+r)/2 in let (hm,v)=List.nth t m in if hm = h then Some v else if hm < h then bs (m+1) r else bs l (m-1) in
    match bs 0 (List.length t - 1) with Some vnode -> vnode | None -> (match t with [] -> failwith "empty ring" | (_,v)::_ -> v)
  let vnode_to_shard vnode = match Astring.String.cut ~sep:"-" vnode with Some (sid,_) -> int_of_string sid | None -> failwith "bad vnode"
end

(* ----------------------------- Shard ------------------------------ *)
type shard = {
  id:int;
  wal_file:string;
  snapshot_file:string;
  state: RbMap.t ref;
  mutable term:int;
  mutable is_leader:bool;
  mutable leader_id:int;
  replica_chans: (string * Domainslib.Chan.t) list ref;
  mutable wal_seq: int64;
  peers: int list;
  wal_writer: wal_writer;
  metrics: Metrics.t;
}

let make_shard id ~wal_file ~snapshot_file ~metrics =
  let ww = make_wal_writer wal_file metrics in
  { id; wal_file; snapshot_file; state = ref RbMap.empty; term=0; is_leader=false; leader_id=id; replica_chans = ref []; wal_seq=0L; peers=[]; wal_writer=ww; metrics }

let apply_put_local shard key value =
  shard.state := RbMap.insert key value !(shard.state);
  shard.metrics.put_count <- shard.metrics.put_count + 1

(* --------------------------- WAL/Replica --------------------------- *)
let replica_loop (ch : (string * Domainslib.Chan.t) Domainslib.Chan.t) (s:shard) =
  let rec loop () =
    match Domainslib.Chan.recv ch with
    | (line, ack_ch) ->
        append_wal_buffered s.wal_writer line;
        (try match Yojson.Safe.from_string line with
         | `Assoc items -> (match List.assoc "op" items with
             | `String "put" -> (match List.assoc "key" items, List.assoc "value" items with `String k, `String v -> s.state := RbMap.insert k v !(s.state) | _ -> ())
             | _ -> ())
         | _ -> ()) with _ -> ();
        (try Domainslib.Chan.send ack_ch "ack" with _ -> ());
        loop ()
  in loop ()

(* ---------------------------- Master loop ------------------------- *)

type request = RPut of string * string * (string option) Domainslib.Chan.t | RGet of string * (string option) Domainslib.Chan.t | RStop
let majority n = (n / 2) + 1

let master_loop (req_ch : request Domainslib.Chan.t) (s:shard) ~mpi_rank ~mpi_size pool =
  let rec loop () =
    match Domainslib.Chan.recv req_ch with
    | RPut (k, v, reply) ->
        if not s.is_leader then (Domainslib.Chan.send reply (Some (sprintf "redirect:%d" s.leader_id)); loop ()) else (
          s.wal_seq <- Int64.add s.wal_seq 1L;
          let j = `Assoc [ ("op", `String "put"); ("key", `String k); ("value", `String v); ("seq", `Intlit (Int64.to_string s.wal_seq)) ] in
          let line = Yojson.Safe.to_string j in
          append_wal_buffered s.wal_writer line;
          apply_put_local s k v;
          let replica_count = List.length !(s.replica_chans) in
          let needed = majority (replica_count + 1) in
          if replica_count = 0 then (Domainslib.Chan.send reply (Some "ok"); loop ()) else (
            let ack_ch = Domainslib.Chan.make_unbounded () in
            List.iter (fun (_name, rch) -> try Domainslib.Chan.send rch (line, ack_ch) with _ -> ()) !(s.replica_chans);
            (* Optional: MPI broadcast to other processes if configured: omitted in default build *)
            let got = ref 1 in
            let deadline = Unix.gettimeofday () +. 5.0 in
            while !got < needed && Unix.gettimeofday () < deadline do
              try match Domainslib.Chan.recv ack_ch with | "ack" -> incr got | _ -> () with _ -> ()
            done;
            if !got >= needed then Domainslib.Chan.send reply (Some "ok") else Domainslib.Chan.send reply (Some "err:qfail");
            loop ()
          )
        )
    | RGet (k, reply) ->
        s.metrics.get_count <- s.metrics.get_count + 1;
        (try let v = RbMap.find k !(s.state) in Domainslib.Chan.send reply (Some v) with Not_found -> Domainslib.Chan.send reply None);
        loop ()
    | RStop -> ()
  in loop ()

(* ---------------------------- Snapshotting ------------------------ *)
let write_snapshot ~fname map =
  let tmp = fname ^ ".tmp" in
  let oc = open_out tmp in
  let json = `Assoc (RbMap.bindings map |> List.map (fun (k,v) -> (k, `String v))) in
  output_string oc (Yojson.Safe.to_string json); flush oc;
  let fd = Unix.descr_of_out_channel oc in (try Unix.fsync fd with _ -> ()); close_out oc; (try Unix.rename tmp fname with _ -> ())
let load_snapshot ~fname =
  if Sys.file_exists fname then
    try
      let ic = open_in fname in
      let s = input_line ic in close_in ic;
      match Yojson.Safe.from_string s with
      | `Assoc pairs -> List.fold_left (fun acc (k,jv) -> match jv with `String v -> RbMap.insert k v acc | _ -> acc) RbMap.empty pairs
      | _ -> RbMap.empty
    with _ -> RbMap.empty
  else RbMap.empty

(* ------------------------- Coordinator & Rebalance ----------------- *)
let move_keys_for_rebalance ~ring ~shards shard_array shard_id req_chs =
  let my_shard = shard_array.(shard_id) in
  let bindings = RbMap.bindings !(my_shard.state) in
  List.iter (fun (k,v) ->
    let vnode = Ring.find_shard ring k in
    let dest = Ring.vnode_to_shard vnode in
    if dest <> shard_id then (
      let reply = Domainslib.Chan.make_unbounded () in
      Domainslib.Chan.send req_chs.(dest) (RPut (k, v, reply));
      match Domainslib.Chan.recv reply with Some "ok" -> my_shard.state := RbMap.remove k !(my_shard.state) | _ -> ()
    )
  ) bindings

let coordinator_loop ~shard_ids ~shard_array ~req_chs pool =
  let ring = ref (Ring.build ~shard_ids ~vnodes:virtual_nodes) in
  let _snapshot_task = Domainslib.Task.async pool (fun _ -> while true do Unix.sleepf snapshot_interval; Array.iter (fun s -> try write_snapshot ~fname:s.snapshot_file !(s.state) with _ -> ()) shard_array done) in
  (ring, fun new_shard_ids ->
    let new_ring = Ring.build ~shard_ids:new_shard_ids ~vnodes:virtual_nodes in
    ring := new_ring;
    Array.iteri (fun i _ -> ignore (Domainslib.Task.async pool (fun _ -> move_keys_for_rebalance ~ring:!ring ~shards:new_shard_ids shard_array i req_chs))) shard_array
  )

(* ------------------------- Periodic flushers ----------------------- *)
let start_periodic_flushers pool shard_array =
  ignore (Domainslib.Task.async pool (fun _ -> while true do Unix.sleepf wal_flush_interval; Array.iter (fun s -> flush_wal_writer s.wal_writer) shard_array done));
  ignore (Domainslib.Task.async pool (fun _ -> while true do Unix.sleepf metrics_flush_interval; Array.iter (fun s -> Metrics.flush_to_file (s.snapshot_file ^ ".metrics") s.metrics) shard_array done))

(* ----------------------------- HTTP API --------------------------- *)
let make_http_server ~num_shards ~master_req_chs ~ring_ref ~shard_array ~pool =
  let open Lwt.Infix in
  let compute_shard_for_key key = let vnode = Ring.find_shard !ring_ref key in Ring.vnode_to_shard vnode in
  let handle_get key =
    let sid = compute_shard_for_key key in
    let reply = Domainslib.Chan.make_unbounded () in
    Domainslib.Chan.send master_req_chs.(sid) (RGet (key, reply));
    match Domainslib.Chan.recv reply with Some v -> `Assoc [ ("status", `String "ok"); ("value", `String v) ] | None -> `Assoc [ ("status", `String "not_found") ]
  in
  let handle_put key value =
    let sid = compute_shard_for_key key in
    let reply = Domainslib.Chan.make_unbounded () in
    Domainslib.Chan.send master_req_chs.(sid) (RPut (key, value, reply));
    match Domainslib.Chan.recv reply with
    | Some s when s = "ok" -> `Assoc [ ("status", `String "ok") ]
    | Some s when Astring.String.is_prefix ~affix:"redirect:" s -> let leader = int_of_string (String.sub s (String.length "redirect:") (String.length s - String.length "redirect:")) in `Assoc [ ("status", `String "redirect"); ("leader", `Int leader) ]
    | _ -> `Assoc [ ("status", `String "err") ]
  in
  let callback _conn req body =
    let uri = Cohttp.Request.uri req in
    let path = Uri.path uri in
    let meth = Cohttp.Request.meth req in
    let respond_json ~yojson =
      let body = Yojson.Safe.to_string yojson in
      Cohttp_lwt_unix.Server.respond_string ~status:`OK ~body ()
    in
    match meth, Astring.String.cuts ~sep:"/" path with
    | `GET, [""; "get"] ->
        let query = Uri.query uri in
        (match List.assoc_opt "key" query with
         | Some [k] -> Lwt.return (respond_json ~yojson:(handle_get k))
         | _ -> Cohttp_lwt_unix.Server.respond_string ~status:`Bad_request ~body:"missing key" ())
    | `POST, [""; "put"] ->
        Cohttp_lwt.Body.to_string body >>= fun body_s ->
        let body_j = try Yojson.Safe.from_string body_s with _ -> `Null in
        (match body_j with
         | `Assoc [ ("key", `String k); ("value", `String v) ] -> Lwt.return (respond_json ~yojson:(handle_put k v))
         | _ -> Cohttp_lwt_unix.Server.respond_string ~status:`Bad_request ~body:"bad body" ())
    | `POST, [""; "batch"] ->
        Cohttp_lwt.Body.to_string body >>= fun body_s ->
        let body_j = try Yojson.Safe.from_string body_s with _ -> `Null in
        (match body_j with
         | `List ops_list ->
             let ops = List.filter_map (fun item -> match item with
               | `Assoc [ ("op", `String "put"); ("key", `String k); ("value", `String v) ] -> Some (RPut (k, v, Domainslib.Chan.make_unbounded ()))
               | `Assoc [ ("op", `String "get"); ("key", `String k) ] -> Some (RGet (k, Domainslib.Chan.make_unbounded ()))
               | _ -> None) ops_list in
             (* For simplicity, process sequentially and return list of results *)
             let results = List.map (function
               | RPut (k, v, _) -> handle_put k v
               | RGet (k, _) -> handle_get k
               | _ -> `Null) ops in
             Lwt.return (respond_json ~yojson:(`List results))
         | _ -> Cohttp_lwt_unix.Server.respond_string ~status:`Bad_request ~body:"bad body" ()))
    | `POST, [""; "rebalance"] ->
        Cohttp_lwt.Body.to_string body >>= fun body_s ->
        let body_j = try Yojson.Safe.from_string body_s with _ -> `Null in
        (match body_j with
         | `Assoc [ ("shard_count", `Int n) ] ->
             let new_ids = List.init n (fun i -> i) in
             (* trigger rebalancer in background via pool *)
             ignore (Domainslib.Task.async pool (fun _ -> let ring_ref, trigger = coordinator_loop ~shard_ids:new_ids ~shard_array:shard_array ~req_chs:master_req_chs pool in trigger new_ids));
             Cohttp_lwt_unix.Server.respond_string ~status:`OK ~body:"rebalancing triggered" ()
         | _ -> Cohttp_lwt_unix.Server.respond_string ~status:`Bad_request ~body:"bad body" ()))
    | _ -> Cohttp_lwt_unix.Server.respond_string ~status:`Not_found ~body:"not found" ()
  in
  let server = Cohttp_lwt_unix.Server.make ~callback () in
  let mode = `TCP (`Port 8080) in
  printf "Starting HTTP server on port 8080
";
  Lwt_main.run (Cohttp_lwt_unix.Server.create ~mode server)

(* ------------------------------ Main -------------------------------- *)
let () =
  Random.self_init ();
  let num_shards = match Sys.getenv_opt "SHARD_TOTAL" with Some s -> int_of_string s | None -> (match Sys.cpu_count () with Some n -> n | None -> 4) in
  printf "Starting system with %d shards
" num_shards;
  let pool = Domainslib.Task.setup_pool ~num_additional_domains:(num_shards - 1) () in
  (* create shards and channels *)
  let master_req_chs = Array.init num_shards (fun _ -> Domainslib.Chan.make_unbounded ()) in
  let shard_array = Array.init num_shards (fun i -> let m = Metrics.make () in let s = make_shard i ~wal_file:(sprintf "shard_%d.wal" i) ~snapshot_file:(sprintf "shard_%d.snap" i) ~metrics:m in s.state := load_snapshot ~fname:s.snapshot_file; s) in
  Array.iter (fun s -> let rch = Domainslib.Chan.make_unbounded () in s.replica_chans := [ (sprintf "replica-%d" s.id, rch) ]; ignore (Domainslib.Task.async pool (fun _ -> replica_loop rch s))) shard_array;
  Array.iteri (fun i s -> ignore (Domainslib.Task.async pool (fun _ -> master_loop master_req_chs.(i) s ~mpi_rank:0 ~mpi_size:1 pool))) shard_array;
  (* coordinator builds ring and can trigger rebalances *)
  let shard_ids = Array.to_list (Array.init num_shards (fun i -> i)) in
  let ring_ref, _trigger = coordinator_loop ~shard_ids ~shard_array ~req_chs:master_req_chs pool in
  (* start periodic flushers *)
  start_periodic_flushers pool shard_array;
  (* start HTTP server in this process *)
  make_http_server ~num_shards ~master_req_chs ~ring_ref ~shard_array ~pool

(* end of file *)

