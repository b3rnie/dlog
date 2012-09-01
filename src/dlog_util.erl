

now_in_ms() ->
  {Ms, S, Us} = erlang:now(),
  1000000000*Ms + S*1000 + erlang:trunc(Us/1000).
