%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Entrance for all paxos related communication
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(dlog_server).
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
-record(s, { catchup_pid      :: undefined | pid()
           , promises    = [] :: list()
           , accepts     = [] :: list()
           , requests    = [] :: list()
           %% config
           , round_timeout    :: integer()
           , quorum           :: integer()
           , nodes            :: list()
           }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

submit(V, Timeout) ->
  gen_server:call(?MODULE, {submit, V, Timeout}, infinity).

role() ->
  gen_server:call(?MODULE, role).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  erlang:process_flag(trap_exit, true),
  {ok, Nodes}        = application:get_env(dlog, nodes),
  {ok, Id}           = application:get_env(dlog, id),
  {ok, RoundTimeout} = application:get_env(dlog, round_timeout),
  Quorum             = dlog_util:quorum(erlang:length(Nodes)),
  {ok, Pid}          = dlog_catchup:start_link(),
  {ok, #s{catchup_pid=Pid, quorum=Quorum, nodes=Nodes,
          round_timeout=RoundTimeout}}.

terminate(_Rsn, S) ->
  ok.

handle_call({submit, V, _Timeout}, From, #s{fsms=Fsms0} = S) ->
  Slot = dlog_store:next_free_slot(),
  case S#s.is_leader of
    true  -> dlog_transport:broadcast({Slot, node(), {propose, N, V}}, Nodes);
    false -> dlog_transport:broadcast({Slot, node(), {prepare, N}}, Nodes)
  end,
  {noreply, S};

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

%% lease has timed out, try become master
handle_info(lease_timeout, S) ->
  Slot = dlog_store:next_free_slot(),
  dlog_transport:broadcast({Slot, node(), {prepare, N}}, S#s.nodes),
  {noreply, S};

handle_info({Slot, Node, {prepare, N}}, S) ->
  case dlog_store:get_slot_v(Slot) of
    {ok, _V} ->
      dlog_transport:send({Slot, node(), {reject, decided}}, Node),
      {noreply, S};
    {error, no_such_key} ->
      %% 1. Promise to never accept a proposal numbered less than N
      %% 2. Proposal with highest number less than N accepted (if any)
      case dlog_store:get_n(Slot) of
        {ok, Sn}
          when N > Sn ->
          {PrevN, PrevV} =
            case dlog_store:get_accepted(Slot) of
              {ok, {Hn, Hv}}       -> {Hn, Hv};
              {error, no_such_key} -> {null, null}
            end,
          ok = dlog_store:set_n(Slot, N), %% diskwrite + flush
          dlog_transport:send({Slot, node(), {promise, PrevN, PrevV}}, Node),
          {noreply, S};
        {ok, Sn} ->
          dlog_transport:send({Slot, node(), {reject, {seen, Sn}}}, Node),
          {noreply, S};
        {error, no_such_key} ->
          ok = dlog_store:set_n(Slot, N), %% diskwrite + flush
          dlog_transport:send({Slot, node(), {promise, null, null}}, Node),
          {noreply, S}
      end
  end;

handle_info({Slot, Node, {promise, N, {PrevN,PrevV}}}, S) ->
  Promises = [{PrevN,PrevV} | S#s.promises],
  case erlang:length(Promises) >= S#s.quorum of
    true ->
      {HN, HV} = highest_n(Promises, {null,null}),
      if HV =:= null ->
          dlog_store:set_propose(Slot, {S#s.n, S#s.v}), %% diskwrite + flush
          dlog_transport:broadcast({Slot, node(), {propose, N, S#s.v}}, Nodes);
         true ->
          dlog_store:set_propose(Slot, {HN, HV}), %% diskwrite + flush
          dlog_transport:broadcast({Slot, node(), {propose, HN, HV}}, Nodes)
      end;
    false ->
      {noreply, S#s{promises=Promises}}
  end;

handle_info({Slot, Node, {propose, N, V}}, S) ->
  case dlog_store:get_slot_v(Slot) of
    {ok, _} ->
      dlog_store:send({Slot, node(), {reject, decided}}, Node);
    {error, no_such_key} ->
      case dlog_store:get_n(Slot) of
        HN when HN =< N ->
          dlog_store:set_accepted(Slot, N, V),
          dlog_transport:broadcast({Slot, node(), {accepted, Slot, N, V}},
                                   Nodes),
      {noreply, S};
    _ ->
      {noreply, S}
  end.

handle_info({Slot, Node, {accepted, N}}, S) ->
  Accepts = [{Node, N} | S#s.accepts],
  case erlang:length(Accepts) >= S#s.quorum of
    true  ->
      dlog_store:set_slot_v(Slot, V),
      
  {noreply, S};

handle_info({'EXIT', Pid, normal}, #s{catchup_pid=Pid} = S) ->
  {noreply, S#s{catchup_pid=undefined}};

handle_info({'EXIT', Pid, Rsn}, #s{catchup_pid=Pid} = S) ->
  {stop, Rsn, S};

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
insert_fsm(Pid, Slot, Fsms0) ->
  Fsms1 = dict:insert({pid, Pid}, Slot, Fsms0),
  _Fsms = dict:insert({slot, Slot}, Pid, Fsms1).

delete_fsm(Pid, Slot, Fsms0) ->
  Fsms1 = dict:erase({pid, Pid}, Fsms0),
  _Fsms = dict:erase({slot, Slot}, Fsms1).

get_next_n(N, All)-> ((N div All)+1) * All.


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
