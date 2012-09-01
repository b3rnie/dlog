%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc paxos fsm one round
%%%      started
%%%        1. client submitted log
%%%        2. msg from other nodes
%%%        3. fill in gap (noop)
%%%
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
-record(s, { tref
           , id
           , nodes
           , slot
           , quorum
           , v
           }).

%%%_ * API -------------------------------------------------------------
start_link(Slot) ->
  gen_server:start_link(?MODULE, ?MODULE, [Slot], []).

submit(Pid, V) ->
  gen_server:cast(Pid, {submit, V}).

message(Pid, Node, Msg) ->
  gen_server:cast(?MOULE, {Node, Msg}).

%%%_ * gen_server callbacks --------------------------------------------
catchup
follower
master

init([Slot]) ->
  {ok, Nodes}   = application:get_env(dlog, nodes),
  {ok, ID}      = application:get_env(dlog, id),
  {ok, Timeout} = application:get_env(dlog, round_timeout),
  Quorum      = paxos_util:quorom(erlang:length(Nodes)),
  {ok, TRef}  = timer:send_after(Timeout, round_timeout),
  {ok, #s{quorum=Quorum, slot=Slot, nodes=Nodes, id=ID}}.

terminate(_Rsn, S) ->
  ok.

handle_call(stop, _From, S) ->
  {stop, normal, ok, S}.

%% client initiated submission
handle_cast({submit, V}, S) ->
  N = next_sno(0, erlang:length(S#s.nodes), S#s.id),
  %%dlog_store:set_n(Slot, N), %% diskwrite + flush
  dlog_transport:paxos_broadcast({Slot, Node, {prepare, N}}),
  {noreply, S#s{v=V}}.

%% proposer
handle_cast({Node, {promise, N, {PrevN,PrevV}}}, S) ->
  do_promise(Node, N, {PrevN, PrevV}, S);
handle_cast({Node, {reject, N, CurrN}}, S) ->
  {noreply, S};

%% acceptor
handle_cast({Node, {prepare, N}}, S) ->
  do_prepare(Node, N, S);

handle_cast({Node, {propose, N, V}}, S) ->
  do_propose(Node, N, V, S);

%% learner
handle_cast({Node, {accepted, N, V}}, S) ->
  do_accepted(Node, N, V, S).

handle_info(round_timeout, S) ->
  {stop, round_timeout, S};
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals proposer specific -------------------------------------
do_promise(Node, N, {PrevN,PrevV}, S) ->
  Promises = [{PrevN,PrevV} | S#s.promises],
  case erlang:length(Promises) >= S#s.quorum of
    true ->
      {HN, HV} = highest_n(Promises, {null,null}),
      if HV =:= null ->
          dlog_store:set_propose(S#s.slot, {S#s.n, S#s.v}), %% diskwrite + flush
          dlog_transport:broadcast(
            {Slot, Node, {propose, {N, S#s.v}}});
         true ->
          dlog_store:set_propose(S#s.slot, {HN, HV}), %% diskwrite + flush
          dlog_transport:broadcast(
            {Slot, Node, {propose, {HN, HV}}}, Nodes)
      end;
    false ->
      {noreply, S#s{promises=Promises}}
  end.

%%%_ * Internals acceptor specific -------------------------------------
do_prepare(Node, N, #s{slot=Slot} = S) ->
  %% 1. Promise to never accept a proposal numbered less than N
  %% 2. Proposal with highest number less than N accepted (if any)
  %% NOTE: check for already accepted
  case dlog_store:get_n(Slot) of
    {ok, Sn}
      when N > Sn ->
      {PrevN, PrevV} =
        case dlog_store:get_accepted(Slot) of
          {ok, {Hn, Hv}}       -> {Hn, Hv};
          {error, no_such_key} -> {null, null}
        end,
      ok = dlog_store:set_n(Slot, N), %% diskwrite + flush
      dlog_transport:send(Node, {promise, PrevN, PrevV}),
      {noreply, S};
    {ok, Sn} ->
      dlog_transport:send(Node, {reject, Sn}),
      {noreply, S};
    {error, no_such_key} ->
      ok = dlog_store:set_n(Slot, N), %% diskwrite + flush
      dlog_transport:send(Node, {promise, null, null}),
      {noreply, S}
  end.

do_propose(Node, N, V, #s{nodes=Nodes} = S) ->
  %% NOTE: check for already accepted
  case dlog_store:get_n(Slot) of
    HN when HN =< N ->
      dlog_store:set_accepted(Slot, N, V),
      %% dlog_store:set_slot_v(Slot, V),
      %% send accept msg to just coordinator? have coordinator order accept?
      dlog_transport:broadcast(Nodes, {accepted, Slot, N, V}),
      {noreply, S};
    _ ->
      {noreply, S}
  end.

%%%_ * Internals learner specific --------------------------------------
do_accepted(Node, N, V, S) ->
  %% count
  dlog_store:set_slot_v(Slot, V),
  {noreply, S}.

%%%_ * -----------------------------------------------------------------

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

