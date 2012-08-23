%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(paxos_proposer).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start_link/1
        , stop/1
        , next_sno/3
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
-record(s, { v
           , promises=[]
           , quorum
           , slot
           , n
           , client
           , nodes
           }).

%%%_ * API -------------------------------------------------------------
start_link(V) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [self(), V], []).

stop(Pid) ->
  gen_server:cast(Pid, stop).

%%%_ * gen_server callbacks --------------------------------------------
init([Pid, V]) ->
  {ok, ID}    = application:get_env(paxos, id),
  {ok, Nodes} = application:get_env(paxos, nodes),
  Quorum      = paxos_util:quorom(length(Nodes)),
  Slot        = paxos_store:next_slot(),
  N           = next_sno(0, length(Nodes), ID),

  dlog_store:set_n(Slot, N), %% diskwrite + flush
  paxos_util:broadcast({prepare, Slot, N}, Nodes),
  {ok, #s{v=V, quorum=Quorum, slot=Slot, n=N, client=Pid, nodes=Nodes}}.

terminate(_Rsn, _S) ->
  ok.

handle_call(stop, _From, S) ->
  {stop, normal, ok, S}.

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

%% gather promises
handle_info({Node, {promise, LN, LV}}, #s{nodes=Nodes} = S) ->
  Promises = [{LN,LV} | S#s.promises],
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
  end;

handle_info({reject, SN}, S) ->
  {noreply, S};

handle_info({accepted, Slot, N, V}, S) ->
  ok = dlog_store:set_slot_v(Slot, V),
  S#s.client ! {self(), ok},
  {stop, normal, S};

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
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

