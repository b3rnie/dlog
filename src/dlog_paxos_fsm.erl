%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc paxos fsm
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(dlog_paxos_fsm).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start_link/0
        ]).

-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
-include_lib("dlog/include/dlog.hrl").

%%%_* Macros ===========================================================
%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { id
           , nodes
           , slot
           , v=nop
           }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%_ * gen_server callbacks --------------------------------------------
init([Slot, V]) ->
  {ok, Nodes} = application:get_env(paxos, nodes),
  {ok, ID}    = application:get_env(paxos, id),
  Quorum      = paxos_util:quorom(length(Nodes)),
  Slot        = paxos_store:next_slot(),
  N           = next_sno(0, length(Nodes), ID),

  %%dlog_store:set_n(Slot, N), %% diskwrite + flush
  paxos_util:broadcast({prepare, Slot, N}, Nodes),
  {ok, #s{v=V, quorum=Quorum, slot=Slot, n=N, client=Pid, nodes=Nodes}}.

terminate(_Rsn, S) ->
  ok.

handle_call(sync, _From, S) ->
  {reply, ok, S}.

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

%% proposer
handle_cast({Node, {promise, N, V}}, S) -> do_promise(Node, N, V, S);

%% acceptor
handle_cast({Node, {prepare, N}}, S)    -> do_prepare(Node, N, S);
handle_cast({Node, {propose, N, V}}, S) -> do_propose(Node, N, V, S);

%% learner
handle_cast({Node, {accepted, N, V}}, S) -> do_accepted(Node, N, V, S).

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals proposer specific -------------------------------------
do_promise(Node, N, V, S) ->
  Promises = [{N,V} | S#s.promises],
  case length(Promises) >= S#s.quorum of
    true ->
      {HN, HV} = highest_n(Promises, {null,null}),
      if HV =:= null ->
          dlog_store:set_propose(S#s.slot, S#s.n, S#s.v), %% diskwrite + flush
          paxos_util:broadcast(
            {propose, S#s.slot, S#s.n, S#s.v}, Nodes);
         true ->
          dlog_store:set_propose(S#s.slot, HN, HV), %% diskwrite + flush
          paxos_util:broadcast({propose, S#s.slot, HN, HV}, Nodes)
      end;
    false ->
      {noreply, S#s{promises=Promises}}
  end.

%%%_ * Internals acceptor specific -------------------------------------
do_prepare(Node, N, #s{slot=Slot} = S) ->
  %% NOTE: check for already accepted
  case dlog_store:get_n(Slot) of
    {ok, Sn}
      when N > Sn ->
      {PrevN, PrevV} =
        case dlog_store:get_accepted(Slot) of
          {ok, {Hn, Hv}} -> {Hn, Hv};
          {error, no_such_key} -> {null, null}
        end,
      ok = dlog_store:set_n(Slot, N),
      paxos_util:send(Node, {promise, PrevN, PrevV}),
      {noreply, S};
    {ok, Sn} ->
      paxos_util:send(Node, {reject, Sn}),
      {noreply, S};
    {error, no_such_key} ->
      ok = dlog_store:set_n(Slot, N),
      paxos_util:send(Node, {promise, null, null}),
      {noreply, S}
  end.

do_propose(Node, N, V, #s{nodes=Nodes} = S) ->
  %% NOTE: check for already accepted
  case dlog_store:get_n(Slot) of
    HN when HN =< N ->
      dlog_store:set_accepted(Slot, N, V),
      %% dlog_store:set_slot_v(Slot, V),
      %% send accept msg to just coordinator? have coordinator order accept?
      paxos_util:broadcast(Nodes, {accepted, Slot, N, V}),
      {noreply, S};
    _ ->
      {noreply, S}
  end.

%%%_ * Internals proposer specific -------------------------------------
do_accepted(Node, N, V, S) ->
  dlog_store:set_slot_v(Slot, V),
  {noreply, S}.

next_sno(N, Tot, ID)
  when (N rem Tot) =:= ID -> N;
next_sno(N, Tot, ID) ->
  next_sno(N+1, Tot, ID).

highest_n([{N,V}|Promises], {null,null}) ->
  highest_n(Promises, {N,V});
highest_n([{N,V}|Promises], {PrevN,PrevV}) ->
  case N>PrevN of
    true  -> highest_n(Promises, {N,V});
    false -> highest_n(Promises, {PrevN,PrevV})
  end;
highest_n([], {N,V}) -> {N,V}.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

next_sno_test() ->
  3 = next_sno(0, 5, 3),
  8 = next_sno(4, 5, 3),
  ok.

highest_n_test() ->
  {3, baz} = highest_n([{1,foo}, {3,baz}, {2,bar}], {null, null}),
  ok.

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:

