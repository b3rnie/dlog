%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright Bjorn Jensen-Urstad 2012
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(dlog).

%%%_* Exports ==========================================================
%% api
-export([ submit/2
        ]).

%%%_* Macros ===========================================================
%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
%% @doc submit value to log
submit(V, Timeout) ->
  dlog_paxos_server:submit(V, Timeout).


  {ok, Pid} = paxos_proposer:start_link(V),
  receive {Pid, Res} -> Res
  after Timeout ->
      paxos_proposer:stop(Pid),
      {error, timeout}
  end.

%%%_ * Internals -------------------------------------------------------

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
