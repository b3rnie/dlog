%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%% @copyright 2012 Bjorn Jensen-Urstad
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(paxos_store).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start_link/0
        , stop/0

        , next_slot/0
        , set_slot_v/2
        ]).

-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Macros ===========================================================
-define(slots_per_file, 5000).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s, { logdir
           , open=[]
           }).

%%%_ * API -------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

next_slot()              -> call({next_slot,    []}).
set_slot_v(Slot, V)      -> call({set_slot_v,   [Slot, V]}).
get_n(Slot)              -> call({get_n,        [Slot]}).
%% REQUIRES diskwrite + flush
set_n(Slot, N)           -> call({set_n,        [Slot, N]}).
%% REQUIRES diskwrite + flush
set_accepted(Slot, N, V) -> call({set_accepted, [Slot, N, V]}).
%% REQUIRES diskwrite + flush
set_propose(Slot, N, V)  -> call({set_propose,  [Slot, N, V]}).

%%%_ * gen_server callbacks --------------------------------------------
init([]) ->
  {ok, LogDir} = application:get_env(paxos, logdir),
  {ok, #s{logdir=LogDir}}.

terminate(_Rsn, S) ->
  lists:foreach(fun(Name) -> ok = dets:close(Name) end, S#s.open),
  ok.

handle_call({Cmd, Args}, _From, S) ->
  {Res, S} = do_cmd(Cmd, Args),
  {reply, Res, S};

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
do_cmd(Cmd, Args, S) ->
  {ok, S}.

filename(LogDir, Slot) ->
  N  = (Slot - (Slot rem ?slots_per_file)) div ?slots_per_file,
  FN = lists:flatten(io_lib:format("~8..0B", [N])),
  filename:join([LogDir, FN]).

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:

