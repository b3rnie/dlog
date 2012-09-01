%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(dlog_paxos_util).

%%%_* Exports ==========================================================
%% api
-export([ send/2
        , broadcast/2
        , quorum/1
        ]).

%%%_* Macros ===========================================================

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
send(Msg, Node) ->
  {dlog_paxos_server, Node} ! Msg.

broadcast(Msg, Nodes) ->
  lists:foreach(fun(Node) -> {dlog_paxos_server, Node} ! Msg end, Nodes).


now_in_ms() ->
  {Ms, S, Us} = erlang:now(),
  1000000000*Ms + S*1000 + erlang:trunc(Us/1000).

quorum(N)
  when erlang:is_integer(N), N > 0 ->
  erlang:trunc(N/2)+1.

%%%_ * Internals -------------------------------------------------------
%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

quorum_test() ->
  1 = quorum(1),
  2 = quorum(2),
  2 = quorum(3),
  3 = quorum(4),
  3 = quorum(5),
  4 = quorum(6),
  4 = quorum(7),
  ok.

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
