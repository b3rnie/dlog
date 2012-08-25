%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(dlog_paxos_transport).

%%%_* Exports ==========================================================
%% api
-export([ send/2
        , broadcast/2
        ]).

%%%_* Macros ===========================================================

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
send(Msg, Node) ->
  {dlog_paxos_server, Node} ! Msg.

broadcast(Msg, Nodes) ->
  lists:foreach(fun(Node) -> {dlog_paxos_server, Node} ! Msg end, Nodes).

%%%_ * Internals -------------------------------------------------------
%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
