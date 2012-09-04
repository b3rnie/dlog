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
-record(s, { catchup_pid      = undefined
           , requests         = queue:new()
           %% paxos
           , is_leader        = false
           , current_slot     = undefined
           , current_n        = undefined
           , current_promises = undefined
           , current_accepts  = undefined
           , current_from     = undefined
           %% config
           , timeout          = undefined
           , quorum           = undefined
           , nodes            = undefined
           , id               = undefined
           }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

submit(V, Timeout) ->
  gen_server:call(?MODULE, {submit, V, Timeout}, infinity).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  erlang:process_flag(trap_exit, true),
  {ok, Nodes}   = application:get_env(dlog, nodes),
  {ok, Id}      = application:get_env(dlog, id),
  {ok, Timeout} = application:get_env(dlog, timeout),
  Quorum        = dlog_util:quorum(erlang:length(Nodes)),
  {ok, Pid}     = dlog_catchup:start_link(),
  {ok, #s{ nodes       = Nodes
         , id          = Id
         , timeout     = Timeout
         , quorum      = Quorum
         , catchup_pid = Pid
         }}.

terminate(_Rsn, S) ->
  ok.

handle_call({submit, V, Timeout}, From,
            #s{ current_slot     = undefined
              , current_n        = undefined
              , current_promises = undefined
              , current_accepts  = undefined
              , nodes            = Nodes
              , catchup_pid      = undefined
              } = S) ->
  ?hence(queue:is_empty(S#s.requests)),
  Slot = dlog_store:next_free_slot(),
  N    = next_sno(0, erlang:length(Nodes), S#s.id),
  case S#s.is_leader of
    true  ->
      %% skip prepare-promise if this node was leader of last instance
      %% (multi-paxos).
      Msg = {Slot, node(), {propose, N, V}},
      dlog_transport:broadcast(Msg, Nodes),
     false ->
      Msg = {Slot, node(), {prepare, N}},
      dlog_transport:broadcast(Msg, Nodes),
  end,
  {noreply, S#s{current_slot     = Slot,
                current_n        = N,
                current_promises = [],
                current_accepts  = [],
                current_from     = From
               }};
handle_call({submit, V, Timeout}, From, #s{catchup_pid=undefined} = S) ->
  %% enqueue and do later
  {noreply, S#s{requests=queue:in({V, Timeout, From}, S#s.requests)}};
handle_call({submit, _V, _Timeout}, _From, S) ->
  %% node is participating as a voting member but will not submit
  %% any new entries to log until catchup is done.
  {reply, {error, catching_up}, S}.

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

handle_info({Slot, Node, {prepare, N}}, S) ->
  case dlog_store:get_slot_v(Slot) of
    {ok, _V} ->
      dlog_transport:send({Slot, node(), {reject, slot_decided}}, Node),
      {noreply, S};
    {error, no_such_key} ->
      %% 1. Promise to never accept a proposal numbered less than N
      %% 2. Proposal with highest number less than N accepted (if any)
      case dlog_store:get_n(Slot) of
        {ok, Sn}
          when N > Sn ->
          {Pn, Pv} =
            case dlog_store:get_accepted(Slot) of
              {ok, {Pn, Pv}}       -> {Pn,Pv};
              {error, no_such_key} -> {null,null}
            end,
          ok = dlog_store:set_n(Slot, N), %% diskwrite + flush
          Msg = {Slot, node(), {promise, Pn, Pv}},
          dlog_transport:send(Msg, Node),
          {noreply, S};
        {ok, Sn} ->
          Msg = {Slot, node(), {reject, seen, Sn}},
          dlog_transport:send(Msg, Node),
          {noreply, S};
        {error, no_such_key} ->
          ok = dlog_store:set_n(Slot, N), %% diskwrite + flush
          Msg = {Slot, node(), {promise, null, null}},
          dlog_transport:send(Msg, Node),
          {noreply, S}
      end
  end;

handle_info({Slot, Node, {promise, N, {Pn,Pv}}},
            #s{current_slot=CurrentSlot} = S)
  when Slot =/= CurrentSlot ->
  %% promise but not working on it, ignore.
  {noreply, S};
handle_info({Slot, Node, {promise, N, {Pn,Pv}}},
            #s{current_n = CurrentN} = S)
  when N =/= CurrentN ->
  %% promise for a different N
  {noreply, S};
handle_info({Slot, Node, {promise, N, {Pn,Pv}}},
            #s{ current_slot = Slot
              , current_n    = N
              } = S) ->
  Promises = [{Pn,Pv} | S#s.current_promises],
  case erlang:length(Promises) >= S#s.quorum of
    true ->
      case promise_max(Promises) of
        {null,null} ->
          %% free to pick what to propose
          %% diskwrite + flush
          dlog_store:set_propose(Slot, {N, S#s.current_v}),
          Msg = {Slot, node(), {propose, N, S#s.current_v}},
          dlog_transport:broadcast(Msg, S#s.nodes);
        {NMax,VMax} ->
          %% bound by earlier promises done
          %% diskwrite + flush
          dlog_store:set_propose(Slot, {NMax,VMax}),
          Msg = {Slot, node(), {propose, NMax, VMax}},
          dlog_transport:broadcast(Msg, S#s.nodes)
      end;
    false ->
      {noreply, S#s{current_promises=Promises}}
  end;

handle_info({Slot, Node, {propose, N, V}}, S) ->
  case dlog_store:get_slot_v(Slot) of
    {ok, _} ->
      dlog_store:send({Slot, node(), {reject, decided}}, Node);
    {error, no_such_key} ->
      case dlog_store:get_n(Slot) of
        HN when HN =< N ->
          dlog_store:set_accepted(Slot, N, V),
          Msg = {Slot, node(), {accepted, N}},
          dlog_transport:broadcast(Msg, Nodes),
          {noreply, S};
        _ ->
      {noreply, S}
  end.

handle_info({Slot, Node, {accepted, N}},
            #s{current_n = CurrentN} = S)
  when N =/= CurrentN ->
  {noreply, S};
handle_info({Slot, Node, {accepted, N}}, S) ->
  Accepts = lists:usort([Node | S#s.current_accepts]),
  case erlang:length(Accepts) >= S#s.quorum of
    true  ->
      {ok, V} = dlog_store:get_accepted(Slot, N),
      dlog_store:set_slot_v(Slot, V),
      %% done move on to next slot
      case queue:out(S#s.requests) of
        {{value, Item}, Requests} ->
          %% todo send request
          {noreply, S};
        {empty, S#s.requests} ->
          {noreply, S#s{current_slot=undefined,
                        current_n=N,
                        current_promises=[],
                        current_accepts=[],
                        current_from=From}}
      end;
    false ->
      {noreply, S#s{current_accepts=Accepts}}
  end;

handle_info({Slot, Node, {reject, slot_decided}}, S) ->
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
next_sno(N, Tot, ID)
  when (N rem Tot) =:= ID -> N;
next_sno(N, Tot, ID) ->
  next_sno(N+1, Tot, ID).

promise_max(Promises) ->
  lists:foldl(fun({N,V}, {null,null})       -> {N,V};
                 ({N,V}, {Pn,Pv}) when N>Pn -> {N,V};
                 ({N,V}, {Pn,Pv})           -> {Pn,Pv}
              end, {null, null}, Promises).

insert_fsm(Pid, Slot, Fsms0) ->
  Fsms1 = dict:insert({pid, Pid}, Slot, Fsms0),
  _Fsms = dict:insert({slot, Slot}, Pid, Fsms1).

delete_fsm(Pid, Slot, Fsms0) ->
  Fsms1 = dict:erase({pid, Pid}, Fsms0),
  _Fsms = dict:erase({slot, Slot}, Fsms1).

get_next_n(N, All)-> ((N div All)+1) * All.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

next_sno_test() ->
  3 = next_sno(0, 5, 3),
  8 = next_sno(4, 5, 3),
  ok.

promise_max_test() ->
  {3, baz} = promise_max([{1,foo}, {3,baz}, {2,bar}], {null, null}),
  ok.

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
