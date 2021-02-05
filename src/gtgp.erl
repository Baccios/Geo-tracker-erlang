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
-export([spawn_rm/1, spawn_rm/0]).

% spawns a replica manager on the current node
% param Dispatchers: The list of nodes where dispatchers are spawned
spawn_rm(Dispatchers) ->
  gen_server:start({local, rm_gossip_sender}, rm_gossip_sender, [], []),
  gen_server:start({local, rm_gossip_reception}, rm_gossip_reception, [], []),
  gen_server:start({local, rm_map_server}, rm_map_server, Dispatchers, []).

spawn_rm() ->
  spawn_rm([]).
