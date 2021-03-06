%%%-------------------------------------------------------------------
%%% @author L. Bacciottini, F. Pacini
%%% @copyright (C) 2021, UNIPI
%%% @doc
%%%
%%% @end
%%% Created : 05. Feb 2021 3:10 PM
%%%-------------------------------------------------------------------
-module(gtgp).
-author("L. Bacciottini, F. Pacini").

%% API
-export([spawn_rm/1, spawn_rm/0, spawn_dispatcher/1, init_dispatcher_static/1, init_dispatcher_test/1]).
-define(HOST,'@localhost').

% spawns a replica manager on the current node
% param Dispatchers: The list of nodes where dispatchers are spawned
spawn_rm(Dispatchers) ->
  gen_server:start({local, rm_gossip_sender}, rm_gossip_sender, [], []),
  gen_server:start({local, rm_gossip_reception}, rm_gossip_reception, [], []),
  gen_server:start({local, rm_map_server}, rm_map_server, Dispatchers, []).

spawn_rm() ->
  spawn_rm([]).

spawn_dispatcher(Neighbours_list) -> %Total = total number of dispatcher, Index = ith position in total
  gen_server:start({local, dispatcher}, dispatcher, Neighbours_list, []).

%%Use as Neigh_List = gtgp:init_dispatcher_static(5).
%%Then, pass it to dispatchers gtgp:spawn_dispatcher(1..5,NL). where erl -sname d1..5@localhost
init_dispatcher_static(Total_number_of_dispatcher) ->
  init_dispatcher_static(Total_number_of_dispatcher, 1).

init_dispatcher_static(Total,Current_step) ->
  if
    Current_step =< Total ->
      [
        list_to_atom(
          atom_to_list('d') ++
            atom_to_list(binary_to_atom(list_to_binary(integer_to_list(Current_step)),utf8)) ++
            atom_to_list(?HOST)
        )
      ] ++ init_dispatcher_static(Total, Current_step + 1);
    true -> []
  end.

%% to be used on final deployment
%%Use as Neigh_List = gtgp:init_dispatcher_test(5).
%%Then, pass it to dispatchers gtgp:spawn_dispatcher(1..5,NL). where erl -sname d1..5@dispatcher1..5
init_dispatcher_test(Total_number_of_dispatcher) ->
  init_dispatcher_test(Total_number_of_dispatcher, 1).
init_dispatcher_test(Total, Current_step) ->
  if
    Current_step =< Total ->
      Index = atom_to_list(binary_to_atom(list_to_binary(integer_to_list(Current_step)),utf8)),
      [
        list_to_atom(
          atom_to_list('d') ++
            Index ++
            atom_to_list('@dispatcher') ++
            Index
        )
      ] ++ init_dispatcher_test(Total, Current_step + 1);
    true -> []
  end.