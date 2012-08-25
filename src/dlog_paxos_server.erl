%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Entrance for all paxos related communication
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(dlog_paxos_server).
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
-record(s, {fsms=dict:new()
           }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

submit(V, Timeout) ->
  gen_server:call(?MODULE, {submit, V, Timeout}, infinity).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  erlang:process_flag(trap_exit, true),
  {ok, #s{nodes=Nodes, id=ID}}.

terminate(_Rsn, S) ->
  ok.

handle_call({submit, V, _Timeout}, From, #s{fsms=Fsms0} = S) ->
  %% no timeouts for now
  %% no replies for now
  Slot = dlog_store:get_next_slot(),
  {ok, Pid} = dlog_paxos_fsm:start_link(Slot),
  ok = dlog_paxos_fsm:submit(Pid, V),
  Fsms = insert_fsm(Pid, Slot, S#s.fsms),
  {ok, _} = timer:send_after(15000, {stop_fsm, {Slot, Pid}}),
  {reply, ok, S#s{fsms=Fsms}}.

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

handle_info({paxos, {Slot, Msg}}, S) ->
  case dict:find({slot, Slot}, S#s.fsms) of
    {ok, Pid} -> dlog_paxos_fsm:message(Pid, Msg),
                 {noreply, S};
    error     -> {ok, Pid} = dlog_paxos_fsm:start_link(Slot),
                 Fsms = insert_fsm(Pid, Slot, S#s.fsms),
                 {noreply, S#s{fsms=Fsms}}
  end;
handle_info({'EXIT', Pid, normal}, S) ->
  Slot = dict:fetch(Pid, S#s.fsms),
  Pid  = dict:fetch(Slot, S#s.fsms),
  Fsms = delete_fsm(Pid, Slot, S#s.fsms),
  {noreply, S#s{fsms=Fsms}};
handle_info({'EXIT', Pid, Rsn}, S) ->
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
