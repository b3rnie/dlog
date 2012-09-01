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
-record(s, { role            %% master | slave
           , catchup_pid
           , heartbeat_tref
           , lease_tref
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
  {ok, Pid} = dlog_catchup:start_link(),
  {ok, #s{role=slave, catchup_pid=Pid}}.

terminate(_Rsn, S) ->
  ok.

handle_call({submit, _V, _Timeout}, From, #s{state=slave} = S) ->
  {reply, {error, is_slave}, S};
handle_call({submit, V, _Timeout}, From, #s{fsms=Fsms0} = S) ->
  Slot = dlog_store:next_free_slot(),
  dlog_transport:broadcast({Slot, node(), {propose, N, V}}),
  {noreply, S};

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

%% lease has timed out, try become master
handle_info(lease_timeout, S) ->
  Slot = dlog_store:next_free_slot(),
  dlog_transport:broadcast({Slot, node(), {prepare, N}}, S#s.nodes),
  {noreply, S};

handle_info({Slot, Node, {prepare, N}}, S) ->
  {noreply, S};

handle_info({Slot, Node, {promise, N, {PN,PV}}}, S) ->
  {noreply, S};

handle_info({Slot, Node, {propose, N, V}}, S) ->
  {noreply, S}

handle_info({Slot, Node, {accepted, N}}, S) ->
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
