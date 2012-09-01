%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Keep connections up
%%%      Will hopefully simplify testing in future.. but for now
%%%      dist_auto_connect = true
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(dlog_sync).
-behaviour(gen_server).

%%%_* Exports ==========================================================
-export([ start_link/0
        , send/2
        , broadcast/2
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
-define(retry, 15000). %% 15s

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, {nodes=[], connected=[], disconnected=[]}).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

send(Msg, Node) ->
  {dlog_server, Node} ! Msg.

broadcast(Msg, Nodes) ->
  lists:foreach(fun(Node) -> {dlog_server, Node} ! Msg end, Nodes).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  {ok, Nodes} = application:get_env(paxos, nodes),
  ok = net_kernel:monitor_nodes(true),
  {ok, #s{nodes=Nodes, disconnected=Nodes}, 0}.

terminate(_Rsn, S) ->
  ok = net_kernel:monitor_nodes(false),
  ok.

handle_call(sync, _From, S) ->
  {reply, ok, S, ?retry}.

handle_cast(_Msg, S) ->
  {stop, bad_cast, S}.

handle_info(timeout, #s{disconnected=[]} = S) ->
  {noreply, S};
handle_info(timeout, S) ->
  {Connected, Disconnected} =
    lists:foldl(fun(Node, {Connected, Disconnected}) ->
                    case net_kernel:connect_node(Node) of
                      true  -> {[Node|Connected], Disconnected};
                      false -> {Connected, [Node|Disconnected]}
                    end
                end, {S#s.connected, []},
                S#s.disconnected),
  {noreply, S#s{connected=Connected, disconnected=Disconnected}, ?retry};
handle_info({Status, Node}, S)
  when Status =:= nodeup;
       Status =:= nodedown ->
  ?hence(lists:member(Node, S#s.nodes)),
  {Connected, Disconnected} =
    case Status of
      nodeup   -> {ensure(Node, S#s.connected), delete(Node, S#s.disconnected)};
      nodedown -> {delete(Node, S#s.connected), ensure(Node, S#s.disconnected)}
    end,
  {noreply, S#s{disconnected=Disconnected, connected=Connected}, ?retry};
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S, ?retry}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S, ?retry}.

%%%_ * Internals -------------------------------------------------------
ensure(V, L) -> lists:usort([V|L]).
delete(V, L) -> lists:delete(V, L).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
