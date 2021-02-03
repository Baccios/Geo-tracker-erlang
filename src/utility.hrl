%%%-------------------------------------------------------------------
%%% @author L. Bacciottini
%%% @copyright (C) 2021, UNIPI
%%% @doc
%%%
%%% @end
%%% Created : 03. Feb 2021 12:51 PM
%%%-------------------------------------------------------------------
-author("L. Bacciottini").
-record(config, {version, fanout, max_neighbours, sub_probability}).

format_conf(#config{version = V, fanout = F, max_neighbours = M, sub_probability = S}) ->
  io:format("version: ~w, fanout: ~w, max_neighbours = ~w, sub_probability = ~w", [V,F,M,S]).

