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
        , stop/0
        ]).

-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
-include_lib("paxos/include/paxos.hrl").

%%%_* Macros ===========================================================
%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { v
           , promises=[]
           , quorum
           , slot
           , n
           , client
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
  N           = next_sno(N, length(Nodes), ID),

  %%  paxos_store:set_seen_n(N), %% diskwrite + flush
  paxos_util:broadcast({prepare, Slot, N}, Nodes),
  {ok, #s{v=V, quorum=Quorum, slot=Slot, n=N, client=Pid}}.

terminate(_Rsn, S) ->
  ok.

handle_call(sync, _From, S) ->
  {reply, ok, S}.

handle_cast(stop, S) ->
  {stop, normal, S}.

%% gather promises
handle_info({promise, LastSeenN, LastAcceptedV}, #s{nodes=Nodes} = S) ->
  Promises = [{LSN,LSV} | S#s.promises],
  case length(Promises) >= S#s.quorum of
    true ->
      {HN, HV} = highest_n(Promises, {null,null}),
      if HV =:= null ->
          paxos_store:set_propose(S#s.slot, S#s.n, S#s.v), %% diskwrite + flush
          paxos_util:broadcast(
            {propose, S#s.slot, S#s.n, S#s.v}, Nodes);
         true ->
          paxos_store:set_propose(S#s.slot, HN, HV), %% diskwrite + flush
          paxos_util:broadcast({propose, S#s.slot, HN, HV}, Nodes)
      end
    false ->
      {noreply, S#s{promises=Promises}}
  end;

handle_info({reject, SN}, S) ->
  {noreply, S};

handle_info({accepted, Slot, N, V}, S) ->
  ok = paxos_store:set_slot_v(Slot, V),
  S#s.client ! {self(), ok},
  {stop, normal, S};

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
next_sno(N, Tot, ID)
  when (N rem Tot+1) =:= ID -> N;
next_sno(N, Tot, ID) ->
  next_sno(N+1, Tot, ID).

highest_n([{LSN,LSV}|Promises], {null,null}) ->
  highest_n(Promises, {LSN,LSV});
highest_n([{LSN,LSV}|Promises], {N,V}) ->
  case N<LSN of
    true  -> highest_n(Promises, {LSN,LSV});
    false -> highest_n(Promises, {N,V})
  end.


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:

