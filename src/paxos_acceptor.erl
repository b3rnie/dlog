%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(paxos_acceptor).
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
-record(s, { max_nodes
           , nodes
           , n
           , id
           }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  {ok, Store} = application:get_env(paxos, store),
  {ok, Nodes} = application:get_env(paxos, nodes),
  {ok, ID}    = application:get_env(paxos, id),
  {ok, #s{nodes=Nodes, id=ID}}.

terminate(_Rsn, S) ->
  ok.

handle_call(sync, _From, S) ->
  {reply, ok, S}.

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

handle_info({Node, {prepare, Slot, N}}, #s{} = S) ->
  case paxos_store:get_n(Slot) of
    SN when N > SN ->
      {LN, LV} = paxos_store:get_accepted(Slot),
      ok = paxos_store:set_n(Slot, N),
      paxos_util:send(Node, {promise, LN, LV}),
      {noreply, S};
    SN ->
      paxos_util:send(Node, {reject, SN}),
      {noreply, S}
  end;

handle_info({Node, {propose, Slot, N, V}}, #s{nodes=Nodes} = S) ->
  case paxos_store:get_n(Slot) of
    N ->
      paxos_store:set_accepted(Slot, N, V),
      paxos_util:broadcast(Nodes, {accepted, Slot, N, V}),
      {noreply, S};
    _ ->
      {noreply, S}
  end;

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------

get_next_n(N, All)-> ((N div All)+1) * All.


%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
