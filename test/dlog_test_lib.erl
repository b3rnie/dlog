%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(dlog_test_lib).

-export([ in_clean_env/1
        ]).

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
in_clean_env(Fun) ->
  _      = application:load(dlog),
  OldEnv = application:get_all_env(dlog),
  clean_files(),
  try Fun()
  after
    del_envs(application:get_all_env(dlog)),
    set_envs(OldEnv),
    clean_files()
  end.

%%%_ * Internals -------------------------------------------------------
del_envs(L) ->
  lists:foreach(fun({K,_}) -> application:unset_env(dlog, K) end, L).

set_envs(L) ->
  lists:foreach(fun({K,V}) -> application:set_env(dlog, K, V) end, L).

clean_files() ->
  {ok, Dir} = application:get_env(dlog, logdir),
  Files     = [dlog_store:index_file(Dir) | dlog_store:log_files(Dir)],
  lists:foreach(fun(File) -> ok = file:delete(File) end, Files).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
