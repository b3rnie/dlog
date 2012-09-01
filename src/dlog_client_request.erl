%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc track/timeout/retry client requests
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(dlog_client_request).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start_link/0
        , submit/2
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
-record(s, { timeout=gb_trees:empty()
           , requests=dict:new()
           }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

submit(V, Timeout) ->
  AbsTimeout = dlog_util:now_in_ms() + Timeout,
  gen_server:call(?MODULE, {submit, V, AbsTimeout}, infinity).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  {ok, #s{}}.

terminate(_Rsn, S) ->
  ok.

handle_call({submit, V, AbsTimeout}, From, #s{} = S) ->
  Slot = dlog_store:get_next_slot(),
  ok   = dlog_store:set_next_slot(Slot+1),
  Timeout = gb_trees:insert({AbsTimeout, Slot}, From, S#s.timeout),
  dlog_paxos_server:start_slot(Slot),
  dlog_paxos_server:try_submit_v(Slot, V),
  {noreply, S#s{timeout=Timeout}}.

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

handle_info({paxos, {Slot, Node, Msg}}, S) ->
  case dict:find({slot, Slot}, S#s.fsms) of
    {ok, Pid} -> dlog_paxos_fsm:message(Pid, Node, Msg),
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

handle_info(timeout, S) ->
  {noreply, S};
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
